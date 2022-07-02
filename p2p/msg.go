package p2p

import (
	"encoding/binary"
	"errors"
	"net"
	"strconv"
	"strings"
)

//method
const METHOD_BUILD_INBOUND = "build_inbound"
const METHOD_BUILD_OUTBOUND = "build_outbound"
const METHOD_PEERLIST = "peerlist"
const METHOD_PING = "ping"
const METHOD_CLOSE = "close"

//msg
const MSG_IP_OVERLAP_ERR = "ip_overlap_err"
const MSG_PORT_ERR = "port_err"
const MSG_APPROVED = "approved"
const MSG_REJECTED = "rejected"
const MSG_PONG = "pong"

func encode_build_conn(port uint16) ([]byte, error) {
	if port > 65535 {
		return nil, errors.New("port range err:" + strconv.Itoa(int(port)))
	}
	port_bytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(port_bytes, port)
	return port_bytes, nil
}

func decode_build_conn(port_bytes []byte) (uint16, error) {
	port := binary.LittleEndian.Uint16(port_bytes)
	if port > 65535 {
		return port, errors.New("port range err:" + strconv.Itoa(int(port)))
	}
	return port, nil
}

func encode_peerlist(plist []*Peer) ([]byte, error) {
	//len uint16 : 2 bytes
	//peer: 4 bytes + 2 bytes (ip + port)

	result := []byte{}

	len_bytes := make([]byte, 2)
	if len(plist) == 0 {
		binary.LittleEndian.PutUint16(len_bytes, 0)
		return len_bytes, nil
	}

	if len(plist) > REMOTE_PEERLIST_LIMIT {
		return nil, errors.New("encode_peerlist overlimit:" + strconv.Itoa(REMOTE_PEERLIST_LIMIT))
	}

	binary.LittleEndian.PutUint16(len_bytes, uint16(len(plist)))

	result = append(result, len_bytes...)

	for _, peer := range plist {

		ipv4 := net.ParseIP(peer.Ip).To4()
		if ipv4 == nil {
			return nil, errors.New("ipv4 format error:" + peer.Ip)
		}

		octets := strings.Split(peer.Ip, ".")
		octet0, _ := strconv.Atoi(octets[0])
		octet1, _ := strconv.Atoi(octets[1])
		octet2, _ := strconv.Atoi(octets[2])
		octet3, _ := strconv.Atoi(octets[3])
		ip_bytes := []byte{byte(octet0), byte(octet1), byte(octet2), byte(octet3)}
		result = append(result, ip_bytes...)

		port_bytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(port_bytes, peer.Port)

		result = append(result, port_bytes...)
	}

	return result, nil

}

func decode_peerlist(pl_bytes []byte) ([]*Peer, error) {
	if len(pl_bytes) < 2 {
		return nil, errors.New("decode_peerlist format error")
	}

	pl_len := binary.LittleEndian.Uint16(pl_bytes[0:2])
	if int(pl_len) > REMOTE_PEERLIST_LIMIT {
		return nil, errors.New("decode_peerlist overlimit:" + strconv.Itoa(REMOTE_PEERLIST_LIMIT))
	}

	if pl_len == 0 {
		return []*Peer{}, nil
	}

	if len(pl_bytes) != 2+int(pl_len)*6 {
		return nil, errors.New("decode_peerlist content length not match")
	}

	result := []*Peer{}

	for i := 0; i < int(pl_len); i++ {
		start_pos := 2 + i*6
		ip := ""
		ip = ip + string(pl_bytes[start_pos:start_pos+1]) + "."
		ip = ip + string(pl_bytes[start_pos+1:start_pos+2]) + "."
		ip = ip + string(pl_bytes[start_pos+2:start_pos+3]) + "."
		ip = ip + string(pl_bytes[start_pos+3:start_pos+4])

		ipv4 := net.ParseIP(ip).To4()
		if ipv4 == nil {
			return nil, errors.New("ipv4 format error," + ip)
		}

		port := binary.LittleEndian.Uint16(pl_bytes[start_pos+4 : start_pos+6])
		if port > 65535 {
			return nil, errors.New("port error," + string(pl_bytes[start_pos+4:start_pos+6]))
		}

		result = append(result, &Peer{
			Ip:   ip,
			Port: port,
		})
	}

	return result, nil

}
