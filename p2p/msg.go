package p2p

import "encoding/binary"

//method
const METHOD_BUILD_CONN = "build_conn"
const METHOD_PEERLIST = "peerlist"
const METHOD_PING = "ping"

//msg
const MSG_IP_OVERLAP_ERR = "ip_overlap_err"
const MSG_OVERLIMIT_ERR = "overlimit_err"
const MSG_PORT_ERR = "port_err"
const MSG_APPROVED = "approved"
const MSG_PONG = "pong"

func encode_build_conn(port uint16) []byte {
	port_bytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(port_bytes, port)
	return port_bytes
}

func decode_build_conn(port_bytes []byte) uint16 {
	return binary.LittleEndian.Uint16(port_bytes)
}
