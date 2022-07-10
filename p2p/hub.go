package p2p

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/coreservice-io/log"
	"github.com/coreservice-io/reference"
)

type HubConfig struct {
	Port            uint16
	Heart_beat_secs int64
	Inbound_limit   uint // set this to be big for seed nodes
	Outbound_limit  uint // ==0 for seed nodes
	Conns_limit     uint // how many connnections can exist to this hub , bigger then >> P2p_outbound_limit
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

	in_bound_peer_conns map[string]*PeerConn //ip => conn
	in_bound_peer_lock  sync.Mutex

	out_bound_peer_conns map[string]*PeerConn //ip =>conn
	out_bound_peer_lock  sync.Mutex

	handlers map[string]func([]byte) []byte

	seed_manager *SeedManager

	ip_black_list map[string]bool //forbid connection from this ip

	table_manager *TableManager //table manager

}

func (hub *Hub) increase_conn_counter(ip string) bool {
	hub.conn_lock.Lock()
	defer hub.conn_lock.Unlock()

	if hub.conn_counter[ip] >= IP_CONN_LIMIT {
		hub.logger.Debugln("increase_conn_counter overlimit", hub.conn_counter[ip])
		return false
	}

	hub.logger.Infoln("increase_conn_counter add before", ip, hub.conn_counter[ip])
	hub.conn_counter[ip] = hub.conn_counter[ip] + 1
	hub.logger.Infoln("increase_conn_counter add after", ip, hub.conn_counter[ip])
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

	if p2p_config == nil {
		return nil, errors.New("p2p has to be initialized")
	}

	if config == nil {
		return nil, errors.New("config empty error")
	}

	if sm == nil || sm.seeds == nil || sm.peer_pool == nil || sm.ref == nil {
		return nil, errors.New("seed manager empty error, check |sm.Seeds|sm.PeerPool|sm.ref|")
	}

	tm, tm_err := new_table_manager(kvdb, logger)
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
		handlers:             map[string]func([]byte) []byte{},
		seed_manager:         sm,
		ip_black_list:        ip_black_list,
		table_manager:        tm,
		id:                   rand.Uint64(),
	}, nil
}

func (hub *Hub) RegisterHandlers(method string, handler func([]byte) []byte) error {
	if _, ok := hub.handlers[method]; ok {
		return errors.New("method already exist")
	}
	hub.handlers[method] = handler
	return nil
}

func (hub *Hub) pop_outbound_target(conn_key uint32) bool {
	str_key := strconv.Itoa(int(conn_key))
	result, _ := hub.ref.Get(str_key)
	if result == nil {
		return false
	} else {
		hub.ref.Delete(str_key)
		return true
	}
}

func (hub *Hub) set_outbound_target() uint32 {
	random_num := rand.Uint32()
	fmt.Println("set_outbound_target ", random_num)
	value := true
	hub.ref.Set(strconv.Itoa(int(random_num)), &value, 1800) //30 minutes
	return random_num
}

func (hub *Hub) start_server() error {

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(int(hub.config.Port)))
	if err != nil {
		return err
	}

	go func() {
		for {

			/////conn incoming ///////
			hub.conn_lock.Lock()
			if len(hub.conn_counter) > int(hub.config.Conns_limit) {
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
			pc := new_peer_conn(&Peer{Ip: ip}, hub.config.Heart_beat_secs, func(pc *PeerConn) {
				pc.closed = true
				if err != nil {
					hub.logger.Errorln("connection close with error:", err)
				}

				hub.out_bound_peer_lock.Lock()
				hub.logger.Infoln("lock1")

				hub.conn_lock.Lock()
				if hub.out_bound_peer_conns[ip] == pc {
					delete(hub.out_bound_peer_conns, ip)
				}

				hub.logger.Infoln("peerconn close callback ", ip, hub.conn_counter[ip])
				hub.conn_counter[ip] = hub.conn_counter[ip] - 1
				hub.logger.Infoln("peerconn close callback ", ip, hub.conn_counter[ip])

				hub.conn_lock.Unlock()
				hub.out_bound_peer_lock.Unlock()
				hub.logger.Infoln("unlock1")
			})

			pc.set_conn(&conn).reg_close().reg_ping(hub).reg_peerlist(hub).reg_build_inbound(hub).reg_build_outbound(hub).run()

			//close the conn which is used for build_conn callback
			time.AfterFunc(time.Duration(hub.config.Heart_beat_secs)*time.Second, func() {
				outb_pc := hub.out_bound_peer_conns[ip]
				if outb_pc != nil && outb_pc.conn == &conn {
					//conn became outbound conn
					hub.logger.Debugln("conn became outbound conn ,won't close it")
				} else {
					pc.close()
				}
			})

		}
	}()

	return nil
}

func (hub *Hub) debug_conn_printing() {

	for {
		time.Sleep(30 * time.Second)
		hub.logger.Infoln("////////////////////////////////////////")

		hub.logger.Infoln("in_bound_peers")
		for _, p := range hub.in_bound_peer_conns {
			hub.logger.Infoln("ip:", p.peer.Ip, "port:", p.peer.Port)
		}

		hub.logger.Infoln("out_bound_peers")

		for _, p := range hub.out_bound_peer_conns {
			hub.logger.Infoln("ip:", p.peer.Ip, "port:", p.peer.Port)
		}

		hub.logger.Infoln("////////////////////////////////////////")
	}

}

func (hub *Hub) Start() {

	hub.start_server()

	//	go deamon_feeler(hub.table_manager)
	//	go deamon_update_new_table_buffer(hub.table_manager)
	//	go deamon_save_kvdb_tried_table(hub.table_manager)
	go deamon_keep_outbounds(hub)
	//	go deamon_refresh_peerlist(hub)

	go hub.debug_conn_printing()

}
