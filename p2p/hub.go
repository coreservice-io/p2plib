package p2p

import (
	"errors"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/coreservice-io/log"
)

type HubConfig struct {
	Hub_peer           *Peer
	P2p_inbound_limit  uint
	P2p_outbound_limit uint
	Conn_pool_limit    uint //how many connnections can exist connection to this hub
}

type Hub struct {
	config *HubConfig
	kvdb   *KVDB
	logger log.Logger
	seeds  []*Peer

	conn_pool      map[string]*net.Conn
	conn_pool_lock sync.Mutex

	in_bound_peer_conns map[string]*PeerConn
	in_bound_peer_lock  sync.Mutex

	out_bound_peer_conns map[string]*PeerConn
	out_bound_peer_lock  sync.Mutex

	hanlder map[string]func([]byte) []byte
}

func NewHub(kvdb *KVDB, seeds []*Peer, config *HubConfig, logger log.Logger) *Hub {
	return &Hub{
		config:               config,
		seeds:                seeds,
		kvdb:                 kvdb,
		logger:               logger,
		conn_pool:            make(map[string]*net.Conn),
		in_bound_peer_conns:  make(map[string]*PeerConn),
		out_bound_peer_conns: map[string]*PeerConn{},
		hanlder:              map[string]func([]byte) []byte{},
	}
}

func (hub *Hub) RegHandler(method string, handler func([]byte) []byte) error {
	if _, ok := hub.hanlder[method]; ok {
		return errors.New("method already exist")
	}
	hub.hanlder[method] = handler
	return nil
}

func (hub *Hub) buildInboundConn(ip string, port int) error {
	////////////try to build inbound connection//////////////////
	hub.in_bound_peer_lock.Lock()

	hub.in_bound_peer_conns[ip] = NewPeerConn(true, &Peer{
		P2p_host: ip,
		P2p_port: port,
	}, func(pc *PeerConn, err error) {
		if err != nil {
			hub.logger.Errorln("inbound connection liveness check error:", err)
		}
		hub.in_bound_peer_lock.Lock()
		delete(hub.in_bound_peer_conns, ip)
		hub.in_bound_peer_lock.Unlock()
	}).SetHubPeer(&Peer{
		P2p_version:             hub.config.Hub_peer.P2p_version,
		P2p_sub_version:         hub.config.Hub_peer.P2p_sub_version,
		P2p_body_max_bytes:      hub.config.Hub_peer.P2p_body_max_bytes,
		P2p_method_max_bytes:    hub.config.Hub_peer.P2p_method_max_bytes,
		P2p_live_check_duration: hub.config.Hub_peer.P2p_live_check_duration,
	})

	hub.in_bound_peer_lock.Unlock()

	dial_err := hub.in_bound_peer_conns[ip].Dial()
	if dial_err != nil {
		hub.in_bound_peer_lock.Lock()
		delete(hub.in_bound_peer_conns, ip)
		hub.in_bound_peer_lock.Unlock()
		return dial_err
	}

	hub.in_bound_peer_conns[ip].Run()
	return nil

}

func (hub *Hub) startServer() error {

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(int(hub.config.Hub_peer.P2p_port)))
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

			hub.conn_pool_lock.Lock()
			ip := conn.RemoteAddr().(*net.TCPAddr).IP.String()
			//////////clear old conn and in/out-bound connections if exist and reassign
			if ibc, ok := hub.in_bound_peer_conns[ip]; ok {
				ibc.Close() // will be cleared by callback function
			}
			if ibc, ok := hub.in_bound_peer_conns[ip]; ok {
				ibc.Close() // will be cleared by callback function
			}
			if _, ok := hub.conn_pool[ip]; ok {
				(*hub.conn_pool[ip]).Close()
			}
			hub.conn_pool[ip] = &conn
			////////////////////////////////////////////////
			hub.conn_pool_lock.Unlock()
			//check if incoming conn is the peer's callback which i try to build a outbound connection with
			outboud_target := true
			if outboud_target {

				hub.out_bound_peer_conns[ip] = NewPeerConn(true, nil, func(pc *PeerConn, err error) {
					if err != nil {
						hub.logger.Errorln("outboud connection liveness check error:", err)
					}
					hub.conn_pool_lock.Lock()
					hub.out_bound_peer_lock.Lock()
					if hub.out_bound_peer_conns[ip] == pc {
						delete(hub.out_bound_peer_conns, ip)
					}
					if hub.conn_pool[ip] == pc.conn {
						delete(hub.conn_pool, ip)
					}
					hub.conn_pool_lock.Unlock()
					hub.out_bound_peer_lock.Unlock()
				})
				hub.out_bound_peer_conns[ip].SetConn(&conn).Run()

			} else {

				pc := NewPeerConn(false, nil, func(pc *PeerConn, err error) {
					if err != nil {
						hub.logger.Errorln("inbound connection error:", err)
					}
					hub.conn_pool_lock.Lock()
					if hub.conn_pool[ip] == pc.conn { //maybe already be replaced , only del the old one
						delete(hub.conn_pool, ip)
					}
					hub.conn_pool_lock.Unlock()
				}).SetConn(&conn)

				pc.rpc_client.Register(METHOD_BUILD_CONN, func(input []byte) []byte {
					//add build conn task
					if hub.in_bound_peer_conns[ip] != nil || hub.out_bound_peer_conns[ip] != nil {
						//already exist do nothing
						return []byte(MSG_IP_OVERLAP_ERR)
					}
					if len(hub.in_bound_peer_conns) > int(hub.config.P2p_inbound_limit) {
						return []byte(MSG_OVERLIMIT_ERR)
					}
					go hub.buildInboundConn(ip, 123)
					return []byte(MSG_APPROVED)
				})

				pc.Run()
				//close the conn which is used for build_conn callback
				time.AfterFunc(hub.config.Hub_peer.P2p_live_check_duration, func() {
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
