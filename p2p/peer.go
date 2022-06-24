package p2p

import (
	"errors"
	"io"
	"net"
	"time"

	"github.com/coreservice-io/byte_rpc"
)

type Peer struct {
	conn           *net.TCPConn
	rpc_client     *byte_rpc.Client
	outbound       bool //either outbound or inbound
	close_callback func(error)
}

func NewPeer(is_outbound bool, conn *net.TCPConn, my_p2p_version uint16, my_p2p_sub_version uint16,
	my_p2p_body_max_bytes uint32, my_p2p_method_max_bytes uint8, my_live_check_duration time.Duration, close_callback func(error)) *Peer {
	return &Peer{
		conn:           conn,
		outbound:       is_outbound,
		close_callback: close_callback,
		rpc_client: byte_rpc.NewClient(io.ReadWriteCloser(conn), &byte_rpc.Config{
			Version:          my_p2p_version,
			Sub_version:      my_p2p_sub_version,
			Body_max_bytes:   my_p2p_body_max_bytes,
			Method_max_bytes: my_p2p_method_max_bytes,
		}).StartLivenessCheck(my_live_check_duration, func(err error) {
			close_callback(err)
		}),
	}
}

func (peer *Peer) Close() {
	peer.close_callback(nil)
}

func (peer *Peer) SendMsg(method string, msg []byte) ([]byte, error) {
	result, err_code := peer.rpc_client.Call(method, msg)
	if err_code != 0 {
		return nil, errors.New(byte_rpc.GetErrMsgStr(uint(err_code)))
	}
	return *result, nil
}
