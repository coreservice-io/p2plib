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

	conn_pool      map[string]*net.Conn
	conn_pool_lock sync.Mutex

	in_bound_peer_conns map[string]*PeerConn
	in_bound_peer_lock  sync.Mutex

	out_bound_peer_conns map[string]*PeerConn
	out_bound_peer_lock  sync.Mutex

	hanlder map[string]func([]byte) []byte

	seed_manager *SeedManager
}

func NewHub(kvdb *KVDB, ref *reference.Reference, sm *SeedManager, config *HubConfig, logger log.Logger) *Hub {
	return &Hub{
		config:               config,
		kvdb:                 kvdb,
		ref:                  ref,
		logger:               logger,
		conn_pool:            make(map[string]*net.Conn),
		in_bound_peer_conns:  make(map[string]*PeerConn),
		out_bound_peer_conns: map[string]*PeerConn{},
		hanlder:              map[string]func([]byte) []byte{},
		seed_manager:         sm,
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

func (hub *Hub) dail_outbound_conn(peer *Peer) error {
	hub.out_bound_peer_lock.Lock()
	defer hub.out_bound_peer_lock.Unlock()

	hub.set_outbound_target(peer.Ip)

	outbound_peer := NewPeerConn(nil, true, &Peer{
		Ip:   peer.Ip,
		Port: peer.Port,
	}, nil)

	err := outbound_peer.Dial()
	if err != nil {
		return err
	}

	_, err = outbound_peer.SendMsg(METHOD_BUILD_CONN, encode_build_conn(peer.Port))
	if err != nil {
		return err
	}

	return nil
}

func (hub *Hub) build_inbound_conn(peer *Peer) error {
	////////////try to build inbound connection//////////////////
	hub.in_bound_peer_lock.Lock()

	inbound_peer := NewPeerConn(hub, true, &Peer{
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

	hub.in_bound_peer_conns[peer.Ip] = inbound_peer
	//register all the handlers
	err := inbound_peer.reg_peerlist().RegisterRpcHandlers(hub.hanlder)
	if err != nil {
		hub.logger.Errorln("RegRpcHandlers error", err)
	}
	hub.in_bound_peer_lock.Unlock()

	/////////dail remote tcp////////
	dial_err := inbound_peer.Dial()
	if dial_err != nil {
		hub.in_bound_peer_lock.Lock()
		if hub.in_bound_peer_conns[inbound_peer.Peer.Ip] == inbound_peer {
			delete(hub.in_bound_peer_conns, inbound_peer.Peer.Ip)
		}
		hub.in_bound_peer_lock.Unlock()
		return dial_err
	}
	////////////////////////////////
	inbound_peer.Run()
	return nil

}

func (hub *Hub) rebuild_last_outbound_conns() {
	plist, err := get_outbounds(*hub.kvdb)
	if err != nil {
		hub.logger.Errorln("rebuild_last_outbound_conns err:", err)
		return
	}

	for _, peer := range plist {
		hub.dail_outbound_conn(peer)
	}

}

func (hub *Hub) start_server() error {

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
				}).SetConn(&conn).reg_ping().reg_peerlist()

				//register all the handlers
				err := out_pc.RegisterRpcHandlers(hub.hanlder)
				if err != nil {
					hub.logger.Errorln("RegRpcHandlers error", err)
				}

				hub.out_bound_peer_conns[ip] = out_pc
				out_pc.Run()
				hub.out_bound_peer_lock.Unlock()

			} else {

				NewPeerConn(hub, false, &Peer{Ip: ip}, func(pc *PeerConn, err error) {
					if err != nil {
						hub.logger.Errorln("inbound connection error:", err)
					}
					hub.conn_pool_lock.Lock()
					if hub.conn_pool[pc.Peer.Ip] == pc.conn { //maybe already be replaced , only del the old one
						delete(hub.conn_pool, pc.Peer.Ip)
					}
					hub.conn_pool_lock.Unlock()
				}).SetConn(&conn).reg_ping().reg_peerlist().reg_build_conn().Run()

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

	hub.start_server()

	//process

	//1. try to connect to old outbound connections which saved in dbkv
	rebuild_wait := make(chan struct{}, 0)
	time.AfterFunc(120*time.Second, func() {
		rebuild_wait <- struct{}{}
	})
	hub.logger.Infoln("try rebuild last outbound connections")
	hub.rebuild_last_outbound_conns()
	<-rebuild_wait

	//2. if not enough outbound connections then try from seeds
	if len(hub.out_bound_peer_conns) == 0 {
		hub.seed_manager.Bootstrap()
	}

	if len(hub.out_bound_peer_conns) == 0 {
		hub.logger.Fatalln("p2p initialization error, try to set new seeds")
	}

}
