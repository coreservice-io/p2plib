package p2p

import (
	"errors"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/coreservice-io/byte_rpc"
)

type Peer struct {
	Ip   string `json:"ip"`
	Port uint16 `json:"port"`
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

func (peerConn *PeerConn) RegisterRpcHandlers(handlers map[string]func([]byte) []byte) error {

	if peerConn.rpc_client == nil {
		return errors.New("rpc_client nil")
	}

	for method_str, m_handler := range handlers {
		err := peerConn.rpc_client.Register(method_str, m_handler)
		if err != nil {
			return err
		}
	}
	return nil
}

func (peerConn *PeerConn) Dial() error {
	if peerConn.conn != nil {
		return nil
	}

	endpoint := peerConn.Peer.Ip + ":" + strconv.FormatUint(uint64(peerConn.Peer.Port), 10)

	dialer := net.Dialer{Timeout: 15 * time.Second}
	conn, err := dialer.Dial("tcp", endpoint)
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

		port, err := decode_build_conn(input)
		if err != nil {
			return []byte(MSG_PORT_ERR)
		}

		pc.Peer.Port = port
		pc.Hub.in_bound_peer_lock.Lock()
		defer pc.Hub.in_bound_peer_lock.Unlock()
		pc.Hub.out_bound_peer_lock.Lock()
		defer pc.Hub.out_bound_peer_lock.Unlock()

		//add build conn task
		if pc.Hub.in_bound_peer_conns[pc.Peer.Ip] != nil || pc.Hub.out_bound_peer_conns[pc.Peer.Ip] != nil {
			//already exist do nothing
			return []byte(MSG_IP_OVERLAP_ERR)
		}
		if len(pc.Hub.in_bound_peer_conns) > int(pc.Hub.config.P2p_inbound_limit) {
			return []byte(MSG_OVERLIMIT_ERR)
		}
		go build_inbound_conn(pc.Hub, pc.Peer)
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

func (pc *PeerConn) reg_close() *PeerConn {
	pc.rpc_client.Register(METHOD_CLOSE, func(input []byte) []byte {
		defer pc.Close()
		return []byte(METHOD_CLOSE)
	})
	return pc
}
