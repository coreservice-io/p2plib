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
}

type Hub struct {
	ConnPool            map[string]*net.Conn
	conn_pool_lock      sync.Mutex
	InBoundPeerConns    map[string]*PeerConn
	in_bound_peers_lock sync.Mutex
	OutBoundPeerConns   map[string]*PeerConn
	Seeds               []Peer
	config              *HubConfig
	KVDB                *KVDB
	Logger              log.Logger
}

func New(kvdb *KVDB, seeds []Peer, config *HubConfig, logger log.Logger) *Hub {
	hub := &Hub{
		ConnPool:          make(map[string]*net.Conn),
		InBoundPeerConns:  make(map[string]*PeerConn),
		OutBoundPeerConns: map[string]*PeerConn{},
		Seeds:             seeds,
		config:            config,
		KVDB:              kvdb,
		Logger:            logger,
	}
	return hub
}

func (hub *Hub) exchangePeers() {

	//1. try from kvdb latest outbound connections

	//2. if all failed then try from seeds

}

func (hub *Hub) buildInboundConn(ip string, port int) {
	hub.in_bound_peers_lock.Lock()
	defer hub.in_bound_peers_lock.Unlock()
	if hub.InBoundPeerConns[ip] != nil || hub.OutBoundPeerConns[ip] != nil {
		//already exist do nothing
		hub.Logger.Errorln("buildInboundCon nerr  ip already exist:" + ip)
		return
	}

	hub.InBoundPeerConns[ip] = NewPeerConn(true, &Peer{
		P2p_host: ip,
		P2p_port: port,
	}, func(err error) {
		if err != nil {
			hub.Logger.Errorln("inbound connection liveness check error:", err)
		}
		hub.in_bound_peers_lock.Lock()
		delete(hub.InBoundPeerConns, ip)
		hub.in_bound_peers_lock.Unlock()
	}).SetHubPeer(&Peer{
		P2p_version:             hub.config.Hub_peer.P2p_version,
		P2p_sub_version:         hub.config.Hub_peer.P2p_sub_version,
		P2p_body_max_bytes:      hub.config.Hub_peer.P2p_body_max_bytes,
		P2p_method_max_bytes:    hub.config.Hub_peer.P2p_method_max_bytes,
		P2p_live_check_duration: hub.config.Hub_peer.P2p_live_check_duration,
	})

	hub.InBoundPeerConns[ip].Start()

}

func (hub *Hub) serverAcceptConn(listener net.Listener) (net.Conn, error) {
	conn, err := listener.Accept()
	if err != nil {
		conn.Close()
		return nil, err
	}

	ip := conn.RemoteAddr().(*net.TCPAddr).IP.String()
	hub.conn_pool_lock.Lock()
	defer hub.conn_pool_lock.Unlock()
	if _, ok := hub.ConnPool[ip]; ok {
		conn.Close()
		return nil, errors.New("connection already exist with same ip:" + ip)
	}

	return conn, nil
}

func (hub *Hub) startServer() error {
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(int(hub.config.Hub_peer.P2p_port)))
	if err != nil {
		return err
	}

	go func() {
		for {
			conn, conn_err := hub.serverAcceptConn(listener)
			if conn_err != nil {
				continue
			}
			ip := conn.RemoteAddr().(*net.TCPAddr).IP.String()

			//check if incoming conn is the peer's callback which i try to build a outbound connection with
			outboud_target := true
			if outboud_target {
				hub.OutBoundPeerConns[ip] = NewPeerConn(true, nil, func(err error) {
					if err != nil {
						hub.Logger.Errorln("outboud connection liveness check error:", err)
					}
					delete(hub.OutBoundPeerConns, ip)
					hub.conn_pool_lock.Lock()
					delete(hub.ConnPool, ip)
					hub.conn_pool_lock.Unlock()
				})

				hub.OutBoundPeerConns[ip].SetConn(&conn).Run()

			} else {

				NewPeerConn(false, nil, func(err error) {
					if err != nil {
						hub.Logger.Errorln("inbound connection error:", err)
					}
					delete(hub.OutBoundPeerConns, ip)
					hub.conn_pool_lock.Lock()
					delete(hub.ConnPool, ip)
					hub.conn_pool_lock.Unlock()
				}).SetConn(&conn).rpc_client.Register("build_conn", func(input []byte) []byte {
					//add build conn task
					go hub.buildInboundConn(ip, 123)
					return []byte("build_conn approved")
				}).Run()

				//close the conn which is used for build_conn callback
				time.AfterFunc(hub.config.Hub_peer.P2p_live_check_duration, func() {
					conn.Close()
					hub.conn_pool_lock.Lock()
					delete(hub.ConnPool, ip)
					hub.conn_pool_lock.Unlock()
				})
			}
		}
	}()

	return nil
}

func (hub *Hub) Start() {

	hub.startServer()

	go hub.exchangePeers()

}
