package p2p

import (
	"errors"
	"net"

	"github.com/coreservice-io/byte_rpc"
)

type Peer struct {
	Addr     net.TCPAddr
	Conn     byte_rpc.Client
	Outbound bool //either outbound or inbound
}

func (peer *Peer) SendMsg(method string, msg []byte) ([]byte, error) {
	result, err_code := peer.Conn.Call(method, msg)
	if err_code != 0 {
		return nil, errors.New(byte_rpc.GetErrMsgStr(uint(err_code)))
	}
	return *result, nil
}
