package p2p

import (
	"errors"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/coreservice-io/byte_rpc"
	"github.com/coreservice-io/log"
)

type HubConfig struct {
	P2p_port                uint
	P2p_version             uint16
	P2p_sub_version         uint16
	P2p_body_max_bytes      uint32
	P2p_method_max_bytes    uint8
	P2p_live_check_duration time.Duration
	P2p_inbound_limit       uint
	P2p_outbound_limit      uint
}

type Hub struct {
	ConnPool            map[string]*net.Conn
	conn_pool_lock      sync.Mutex
	InBoundPeers        map[string]*Peer
	in_bound_peers_lock sync.Mutex
	OutBoundPeers       map[string]*Peer
	Seeds               []Seed
	config              *HubConfig
	KVDB                *KVDB
	Logger              log.Logger
}

func New(kvdb *KVDB, seeds []Seed, config *HubConfig, logger log.Logger) *Hub {
	hub := &Hub{}
	return hub
}

func (hub *Hub) iniConnections() {
	//1. try from kvdb latest outbound connections

	//2. if all failed then try from seed

}

func (hub *Hub) buildInboundConn(ip string, port int) {
	hub.in_bound_peers_lock.Lock()
	defer hub.in_bound_peers_lock.Unlock()
	if hub.InBoundPeers[ip] != nil || hub.OutBoundPeers[ip] != nil {
		//already exist do nothing
		hub.Logger.Errorln("buildInboundCon nerr  ip already exist:" + ip)
		return
	}

	endpoint := ip + ":" + strconv.Itoa(port)
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		hub.Logger.Errorln("buildInboundConn err:" + endpoint)
		return
	}

	hub.InBoundPeers[ip] = NewPeer(true, conn.(*net.TCPConn), hub.config.P2p_version,
		hub.config.P2p_sub_version, hub.config.P2p_body_max_bytes,
		hub.config.P2p_method_max_bytes, hub.config.P2p_live_check_duration, func(err error) {
			if err != nil {
				hub.Logger.Errorln("inbound connection liveness check error:", err)
			}
			delete(hub.InBoundPeers, ip)
		})

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
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(int(hub.config.P2p_port)))
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
				hub.OutBoundPeers[ip] = NewPeer(true, conn.(*net.TCPConn), hub.config.P2p_version,
					hub.config.P2p_sub_version, hub.config.P2p_body_max_bytes,
					hub.config.P2p_method_max_bytes, hub.config.P2p_live_check_duration,
					func(err error) {
						if err != nil {
							hub.Logger.Errorln("outboud connection liveness check error:", err)
						}
						delete(hub.OutBoundPeers, ip)
						hub.conn_pool_lock.Lock()
						delete(hub.ConnPool, ip)
						hub.conn_pool_lock.Unlock()
					})
			} else {
				byte_rpc.NewClient(conn, &byte_rpc.Config{
					Version:          hub.config.P2p_version,
					Sub_version:      hub.config.P2p_sub_version,
					Body_max_bytes:   hub.config.P2p_body_max_bytes,
					Method_max_bytes: hub.config.P2p_method_max_bytes,
				}).
					Register("build_conn", func(input []byte) []byte {
						//add build conn task
						go hub.buildInboundConn(ip, 123)
						return []byte("build_conn approved")
					}).Run()

				//close the conn which is used for build_conn callback
				time.AfterFunc(hub.config.P2p_live_check_duration, func() {
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

	hub.iniConnections()

}
