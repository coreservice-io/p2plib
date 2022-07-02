package p2p

import (
	"errors"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/coreservice-io/log"
	"github.com/coreservice-io/reference"
)

type HubConfig struct {
	Hub_peer                *Peer
	P2p_version             uint16
	P2p_sub_version         uint16
	P2p_body_max_bytes      uint32
	P2p_method_max_bytes    uint8
	P2p_live_check_duration time.Duration
	P2p_inbound_limit       uint // set this to be big for seed nodes
	P2p_outbound_limit      uint // ==0 for seed nodes
	Conn_pool_limit         uint // how many connnections can exist to this hub , bigger then >> P2p_outbound_limit

}

type Hub struct {
	config *HubConfig
	kvdb   *KVDB
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

func NewHub(kvdb *KVDB, ref *reference.Reference, sm *SeedManager, config *HubConfig, logger log.Logger) *Hub {

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
		ip_black_list:        make(map[string]bool),
	}
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

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(int(hub.config.Hub_peer.Port)))
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

			//check increase counter
			if !hub.increase_conn_counter(ip) {
				conn.Close()
			}

			////////////////////////////////////////////////
			pc := NewPeerConn(hub, false, &Peer{Ip: ip}, func(pc *PeerConn, err error) {
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
				if outb_pc != nil && *outb_pc.conn == conn {
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

	go deamon_keep_outbound_conns(hub)

	go deamon_update_outbound_conns(hub)
}
