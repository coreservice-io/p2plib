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
	P2p_inbound_limit       uint
	P2p_outbound_limit      uint
	Conn_pool_limit         uint //how many connnections can exist connection to this hub
}

type Hub struct {
	config *HubConfig
	kvdb   *KVDB
	ref    *reference.Reference
	logger log.Logger
	seeds  map[string]Peer

	conn_pool      map[string]*net.Conn
	conn_pool_lock sync.Mutex

	in_bound_peer_conns map[string]*PeerConn
	in_bound_peer_lock  sync.Mutex

	out_bound_peer_conns map[string]*PeerConn
	out_bound_peer_lock  sync.Mutex

	hanlder map[string]func([]byte) []byte
}

func NewHub(kvdb *KVDB, ref *reference.Reference, seeds map[string]Peer, config *HubConfig, logger log.Logger) *Hub {
	return &Hub{
		config:               config,
		seeds:                seeds,
		kvdb:                 kvdb,
		ref:                  ref,
		logger:               logger,
		conn_pool:            make(map[string]*net.Conn),
		in_bound_peer_conns:  make(map[string]*PeerConn),
		out_bound_peer_conns: map[string]*PeerConn{},
		hanlder:              map[string]func([]byte) []byte{},
	}
}

func (hub *Hub) AddSeed(seed Peer) {
	hub.seeds[seed.Ip] = seed
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

func (hub *Hub) buildInboundConn(peer *Peer) error {
	////////////try to build inbound connection//////////////////
	hub.in_bound_peer_lock.Lock()

	hub.in_bound_peer_conns[peer.Ip] = NewPeerConn(hub, true, &Peer{
		Ip:   peer.Ip,
		Port: peer.Port,
	}, func(pc *PeerConn, err error) {
		if err != nil {
			hub.logger.Errorln("inbound connection liveness check error:", err)
		}
		hub.in_bound_peer_lock.Lock()
		delete(hub.in_bound_peer_conns, peer.Ip)
		hub.in_bound_peer_lock.Unlock()
	})

	hub.in_bound_peer_lock.Unlock()

	dial_err := hub.in_bound_peer_conns[peer.Ip].Dial()
	if dial_err != nil {
		hub.in_bound_peer_lock.Lock()
		delete(hub.in_bound_peer_conns, peer.Ip)
		hub.in_bound_peer_lock.Unlock()
		return dial_err
	}

	hub.in_bound_peer_conns[peer.Ip].Run()
	return nil

}

func (hub *Hub) startServer() error {

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(int(hub.config.Hub_peer.Port)))
	if err != nil {
		return err
	}

	go func() {
		for {

			/////conn incoming ///////
			hub.conn_pool_lock.Lock()
			if len(hub.conn_pool) > int(hub.config.Conn_pool_limit) {
				hub.conn_pool_lock.Unlock()
				time.Sleep(5 * time.Second)
				continue
			}
			hub.conn_pool_lock.Unlock()

			conn, err := listener.Accept() //block here if no connection
			if err != nil {
				conn.Close()
			}

			ip := conn.RemoteAddr().(*net.TCPAddr).IP.String()

			hub.conn_pool_lock.Lock()
			hub.in_bound_peer_lock.Lock()
			hub.out_bound_peer_lock.Lock()

			if ibc, ok := hub.in_bound_peer_conns[ip]; ok { //check to overwrite
				ibc.Close()
				delete(hub.in_bound_peer_conns, ip)
			}
			if ibc, ok := hub.out_bound_peer_conns[ip]; ok { //check to overwrite
				ibc.Close()
				delete(hub.out_bound_peer_conns, ip)
			}
			if _, ok := hub.conn_pool[ip]; ok { //check to overwrite
				(*hub.conn_pool[ip]).Close()
				delete(hub.conn_pool, ip)
			}

			hub.conn_pool[ip] = &conn
			hub.out_bound_peer_lock.Unlock()
			hub.in_bound_peer_lock.Unlock()
			hub.conn_pool_lock.Unlock()

			////////////////////////////////////////////////

			//check if incoming conn is the peer's callback which i try to build a outbound connection with
			if hub.is_outbound_target(ip) {

				hub.out_bound_peer_lock.Lock()

				out_pc := NewPeerConn(hub, true, &Peer{Ip: ip}, func(pc *PeerConn, err error) {
					if err != nil {
						hub.logger.Errorln("outboud connection liveness check error:", err)
					}
					hub.conn_pool_lock.Lock()
					hub.out_bound_peer_lock.Lock()
					if hub.out_bound_peer_conns[pc.Peer.Ip] == pc {
						delete(hub.out_bound_peer_conns, pc.Peer.Ip)
					}
					if hub.conn_pool[pc.Peer.Ip] == pc.conn {
						delete(hub.conn_pool, pc.Peer.Ip)
					}
					hub.conn_pool_lock.Unlock()
					hub.out_bound_peer_lock.Unlock()
				}).SetConn(&conn)

				//register all the handlers
				hub.reg_ping(out_pc).reg_peerlist(out_pc)
				for method, h := range hub.hanlder {
					out_pc.rpc_client.Register(method, h)
				}

				hub.out_bound_peer_conns[ip] = out_pc
				out_pc.Run()
				hub.out_bound_peer_lock.Unlock()

			} else {

				pc := NewPeerConn(hub, false, &Peer{Ip: ip}, func(pc *PeerConn, err error) {
					if err != nil {
						hub.logger.Errorln("inbound connection error:", err)
					}
					hub.conn_pool_lock.Lock()
					if hub.conn_pool[pc.Peer.Ip] == pc.conn { //maybe already be replaced , only del the old one
						delete(hub.conn_pool, pc.Peer.Ip)
					}
					hub.conn_pool_lock.Unlock()
				}).SetConn(&conn)

				hub.reg_ping(pc).reg_peerlist(pc).reg_build_conn(pc)

				pc.Run()
				//close the conn which is used for build_conn callback
				time.AfterFunc(hub.config.P2p_live_check_duration, func() {
					conn.Close()
				})
			}
		}
	}()

	return nil
}

func (hub *Hub) Start() {

	hub.startServer()

	//go hub.exchangePeers()

}

func (hub *Hub) RegHandler(method string, handler func([]byte) []byte) error {
	if _, ok := hub.hanlder[method]; ok {
		return errors.New("method already exist")
	}
	hub.hanlder[method] = handler
	return nil
}

func (hub *Hub) reg_build_conn(pc *PeerConn) *Hub {
	pc.rpc_client.Register(METHOD_BUILD_CONN, func(input []byte) []byte {

		hub.in_bound_peer_lock.Lock()
		defer hub.in_bound_peer_lock.Unlock()
		hub.out_bound_peer_lock.Lock()
		defer hub.out_bound_peer_lock.Unlock()

		//get the peer port here
		pc.Peer.Port = 8099
		//add build conn task
		if hub.in_bound_peer_conns[pc.Peer.Ip] != nil || hub.out_bound_peer_conns[pc.Peer.Ip] != nil {
			//already exist do nothing
			return []byte(MSG_IP_OVERLAP_ERR)
		}
		if len(hub.in_bound_peer_conns) > int(hub.config.P2p_inbound_limit) {
			return []byte(MSG_OVERLIMIT_ERR)
		}
		go hub.buildInboundConn(pc.Peer)
		return []byte(MSG_APPROVED)
	})
	return hub
}

func (hub *Hub) reg_peerlist(pc *PeerConn) *Hub {
	pc.rpc_client.Register(METHOD_PEERLIST, func(input []byte) []byte {
		return []byte("this is a list of peers")
	})
	return hub
}

func (hub *Hub) reg_ping(pc *PeerConn) *Hub {
	pc.rpc_client.Register(METHOD_PING, func(input []byte) []byte {
		return []byte(MSG_PONG)
	})
	return hub
}
