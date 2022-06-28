package p2p

import (
	"errors"
	"io"
	"net"
	"strconv"

	"github.com/coreservice-io/byte_rpc"
)

type Peer struct {
	Ip   string
	Port int
}

type PeerConn struct {
	Hub            *Hub
	Peer           *Peer
	conn           *net.Conn
	rpc_client     *byte_rpc.Client
	outbound       bool //either outbound or inbound
	close_callback func(*PeerConn, error)
}

func NewPeerConn(hub *Hub, is_outbound bool, peer *Peer, close_callback func(*PeerConn, error)) *PeerConn {
	return &PeerConn{
		Peer:           peer,
		outbound:       is_outbound,
		close_callback: close_callback,
		Hub:            hub,
	}
}

func (peerConn *PeerConn) SetConn(conn *net.Conn) *PeerConn {
	peerConn.conn = conn
	return peerConn
}

func (peerConn *PeerConn) SetRpcHandler(hanlders map[string]func([]byte) []byte) error {
	for method, h := range hanlders {
		err := peerConn.rpc_client.Register(method, h)
		if err != nil {
			return err
		}
	}
	return nil
}

func (peerConn *PeerConn) RegRpcHandlers(handlers map[string]func([]byte) []byte) error {
	if peerConn.rpc_client == nil {
		return errors.New("rpc_client nil")
	}
	for method_str, m_handler := range handlers {
		peerConn.rpc_client.Register(method_str, m_handler)
	}
	return nil
}

func (peerConn *PeerConn) Dial() error {
	if peerConn.conn != nil {
		return nil
	}

	endpoint := peerConn.Peer.Ip + ":" + strconv.Itoa(peerConn.Peer.Port)
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		return errors.New("buildInboundConn err:" + endpoint)
	}

	peerConn.conn = &conn
	return nil
}

func (peerConn *PeerConn) Run() {

	peerConn.rpc_client = byte_rpc.NewClient(io.ReadWriteCloser(*peerConn.conn), &byte_rpc.Config{
		Version:             peerConn.Hub.config.P2p_version,
		Sub_version:         peerConn.Hub.config.P2p_sub_version,
		Body_max_bytes:      peerConn.Hub.config.P2p_body_max_bytes,
		Method_max_bytes:    peerConn.Hub.config.P2p_method_max_bytes,
		Live_check_duration: peerConn.Hub.config.P2p_live_check_duration,
		Conn_closed_callback: func(err error) {
			peerConn.close_callback(peerConn, err)
		},
	})

	peerConn.rpc_client.StartLivenessCheck().Run()
}

func (pc *PeerConn) Close() {
	pc.close_callback(pc, nil)
}

func (pc *PeerConn) SendMsg(method string, msg []byte) ([]byte, error) {
	result, err_code := pc.rpc_client.Call(method, msg)
	if err_code != 0 {
		return nil, errors.New(byte_rpc.GetErrMsgStr(uint(err_code)))
	}
	return *result, nil
}

func (pc *PeerConn) reg_build_conn() *PeerConn {
	pc.rpc_client.Register(METHOD_BUILD_CONN, func(input []byte) []byte {

		pc.Hub.in_bound_peer_lock.Lock()
		defer pc.Hub.in_bound_peer_lock.Unlock()
		pc.Hub.out_bound_peer_lock.Lock()
		defer pc.Hub.out_bound_peer_lock.Unlock()

		//get the peer port here
		pc.Peer.Port = 8099
		//add build conn task
		if pc.Hub.in_bound_peer_conns[pc.Peer.Ip] != nil || pc.Hub.out_bound_peer_conns[pc.Peer.Ip] != nil {
			//already exist do nothing
			return []byte(MSG_IP_OVERLAP_ERR)
		}
		if len(pc.Hub.in_bound_peer_conns) > int(pc.Hub.config.P2p_inbound_limit) {
			return []byte(MSG_OVERLIMIT_ERR)
		}
		go pc.Hub.buildInboundConn(pc.Peer)
		return []byte(MSG_APPROVED)
	})
	return pc
}

func (pc *PeerConn) reg_peerlist() *PeerConn {
	pc.rpc_client.Register(METHOD_PEERLIST, func(input []byte) []byte {
		return []byte("this is a list of peers")
	})
	return pc
}

func (pc *PeerConn) reg_ping() *PeerConn {
	pc.rpc_client.Register(METHOD_PING, func(input []byte) []byte {
		return []byte(MSG_PONG)
	})
	return pc
}
