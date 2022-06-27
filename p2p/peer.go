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
	P2p_host                string
	P2p_port                int
	P2p_version             uint16
	P2p_sub_version         uint16
	P2p_body_max_bytes      uint32
	P2p_method_max_bytes    uint8
	P2p_live_check_duration time.Duration
}

type PeerConn struct {
	Peer           *Peer
	Hub_peer       *Peer
	conn           *net.Conn
	rpc_client     *byte_rpc.Client
	outbound       bool //either outbound or inbound
	close_callback func(*PeerConn, error)
}

func NewPeerConn(is_outbound bool, peer *Peer, close_callback func(*PeerConn, error)) *PeerConn {
	return &PeerConn{
		Peer:           peer,
		outbound:       is_outbound,
		close_callback: close_callback,
	}
}

func (peerConn *PeerConn) SetConn(conn *net.Conn) *PeerConn {
	peerConn.conn = conn
	return peerConn
}

func (peerConn *PeerConn) SetHubPeer(hub_p *Peer) *PeerConn {
	peerConn.Hub_peer = hub_p
	return peerConn
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

	endpoint := peerConn.Peer.P2p_host + ":" + strconv.Itoa(peerConn.Peer.P2p_port)
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		return errors.New("buildInboundConn err:" + endpoint)
	}

	peerConn.conn = &conn
	return nil
}

func (peerConn *PeerConn) Run() {

	peerConn.rpc_client = byte_rpc.NewClient(io.ReadWriteCloser(*peerConn.conn), &byte_rpc.Config{
		Version:             peerConn.Hub_peer.P2p_version,
		Sub_version:         peerConn.Hub_peer.P2p_sub_version,
		Body_max_bytes:      peerConn.Hub_peer.P2p_body_max_bytes,
		Method_max_bytes:    peerConn.Hub_peer.P2p_method_max_bytes,
		Live_check_duration: peerConn.Hub_peer.P2p_live_check_duration,
		Conn_closed_callback: func(err error) {
			peerConn.close_callback(peerConn, err)
		},
	})

	peerConn.rpc_client.StartLivenessCheck().Run()
}

func (peer *PeerConn) Close() {
	peer.close_callback(peer, nil)
}

func (peer *PeerConn) SendMsg(method string, msg []byte) ([]byte, error) {
	result, err_code := peer.rpc_client.Call(method, msg)
	if err_code != 0 {
		return nil, errors.New(byte_rpc.GetErrMsgStr(uint(err_code)))
	}
	return *result, nil
}
