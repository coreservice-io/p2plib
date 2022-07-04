package p2p

import (
	"errors"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/coreservice-io/log"
	"github.com/coreservice-io/reference"
)

type HubConfig struct {
	Hub_port                uint16
	P2p_version             uint16
	P2p_sub_version         uint16
	P2p_body_max_bytes      uint32
	P2p_method_max_bytes    uint8
	P2p_live_check_duration time.Duration
	P2p_inbound_limit       uint // set this to be big for seed nodes
	P2p_outbound_limit      uint // ==0 for seed nodes
	Conn_pool_limit         uint // how many connnections can exist to this hub , bigger then >> P2p_outbound_limit
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Hub struct {
	id uint64

	config *HubConfig
	kvdb   KVDB
	ref    *reference.Reference
	logger log.Logger

	conn_counter map[string]uint8
	conn_lock    sync.Mutex

	in_bound_peer_conns map[string]*PeerConn
	in_bound_peer_lock  sync.Mutex

	out_bound_peer_conns map[string]*PeerConn
	out_bound_peer_lock  sync.Mutex

	hanlder map[string]func([]byte) []byte

	seed_manager *SeedManager

	ip_black_list map[string]bool //forbid connection from this ip

	table_manager *TableManager //table manager

}

func (hub *Hub) increase_conn_counter(ip string) bool {
	hub.conn_lock.Lock()
	defer hub.conn_lock.Unlock()

	if hub.conn_counter[ip] >= IP_CONN_LIMIT {
		return false
	}

	hub.conn_counter[ip] = hub.conn_counter[ip] + 1
	return true
}

func (hub *Hub) AddIpBlackList(ip string) {
	hub.ip_black_list[ip] = true
}

func (hub *Hub) RemoveIpBlackList(ip string) {
	if hub.ip_black_list[ip] {
		delete(hub.ip_black_list, ip)
	}
}

func NewHub(kvdb KVDB, ref *reference.Reference, ip_black_list map[string]bool, sm *SeedManager, config *HubConfig, logger log.Logger) (*Hub, error) {

	if config == nil {
		return nil, errors.New("config empty error")
	}

	if sm == nil || sm.Seeds == nil || sm.PeerPool == nil || sm.Ref == nil {
		return nil, errors.New("seed manager empty error, check |sm.Seeds|sm.PeerPool|sm.ref|")
	}

	tm, tm_err := NewTableManager(kvdb, logger)
	if tm_err != nil {
		return nil, tm_err
	}

	tm.initialize()

	return &Hub{
		config:               config,
		kvdb:                 kvdb,
		ref:                  ref,
		logger:               logger,
		conn_counter:         make(map[string]uint8),
		in_bound_peer_conns:  make(map[string]*PeerConn),
		out_bound_peer_conns: map[string]*PeerConn{},
		hanlder:              map[string]func([]byte) []byte{},
		seed_manager:         sm,
		ip_black_list:        ip_black_list,
		table_manager:        tm,
		id:                   rand.Uint64(),
	}, nil
}

func (hub *Hub) RegisterHandlers(method string, handler func([]byte) []byte) error {
	if _, ok := hub.hanlder[method]; ok {
		return errors.New("method already exist")
	}
	hub.hanlder[method] = handler
	return nil
}

func (hub *Hub) is_outbound_target(ip string) bool {
	key := "outbound_target:" + ip
	result, _ := hub.ref.Get(key)
	if result == nil {
		return false
	} else {
		return true
	}
}

func (hub *Hub) set_outbound_target(ip string) {
	key := "outbound_target:" + ip
	value := true
	hub.ref.Set(key, &value, 1800) //30 minutes
}

func (hub *Hub) start_server() error {

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(int(hub.config.Hub_port)))
	if err != nil {
		return err
	}

	go func() {
		for {

			/////conn incoming ///////
			hub.conn_lock.Lock()
			if len(hub.conn_counter) > int(hub.config.Conn_pool_limit) {
				hub.conn_lock.Unlock()
				time.Sleep(5 * time.Second)
				continue
			}
			hub.conn_lock.Unlock()

			conn, err := listener.Accept() //block here if no connection
			if err != nil {
				conn.Close()
				continue
			}

			ip := conn.RemoteAddr().(*net.TCPAddr).IP.String()
			if hub.ip_black_list[ip] {
				hub.logger.Debugln("ip banned", ip)
				conn.Close()
				continue
			}

			//check increase counter
			if !hub.increase_conn_counter(ip) {
				hub.logger.Debugln("increase_conn_counter failed", ip)
				conn.Close()
			}

			////////////////////////////////////////////////
			pc := NewPeerConn(hub, &Peer{Ip: ip}, func(pc *PeerConn, err error) {
				if err != nil {
					hub.logger.Errorln("connection close with error:", err)
				}

				hub.out_bound_peer_lock.Lock()
				hub.conn_lock.Lock()
				if hub.out_bound_peer_conns[ip] == pc {
					delete(hub.out_bound_peer_conns, ip)
				}
				hub.conn_counter[ip] = hub.conn_counter[ip] - 1
				hub.conn_lock.Unlock()
				hub.out_bound_peer_lock.Unlock()

			}).SetConn(&conn).reg_close().reg_ping().reg_peerlist().reg_build_inbound().reg_build_outbound().Run()

			//close the conn which is used for build_conn callback
			time.AfterFunc(hub.config.P2p_live_check_duration, func() {
				outb_pc := hub.out_bound_peer_conns[ip]
				if outb_pc != nil && outb_pc.conn == &conn {
					//conn became outbound conn
					hub.logger.Debugln("conn became outbound conn ,won't close it")
				} else {
					pc.Close()
				}
			})

		}
	}()

	return nil
}

func (hub *Hub) Start() {

	hub.start_server()

	go deamon_feeler_connection(hub.table_manager)
	go deamon_update_new_table_buffer(hub.table_manager)
	go deamon_save_tried_table(hub.table_manager)
	go deamon_keep_outbound_conns(hub)

}
