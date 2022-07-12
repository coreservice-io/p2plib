package p2p

import (
	"encoding/binary"
	"errors"
	"fmt"
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

func encode_ping(hub_id uint64) []byte {
	hub_id_bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(hub_id_bytes, hub_id)
	return hub_id_bytes
}

func decode_ping(ping_bytes []byte) (uint64, error) {
	if len(ping_bytes) != 8 {
		return 0, errors.New("decode_ping input error")
	}
	return binary.LittleEndian.Uint64(ping_bytes), nil
}

func encode_build_inbound(conn_key uint32) []byte {
	key_bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(key_bytes, conn_key)
	return key_bytes
}

func decode_build_inbound(conn_key_bytes []byte) uint32 {
	return binary.LittleEndian.Uint32(conn_key_bytes)
}

func encode_build_outbound(port uint16, conn_key uint32) []byte {
	port_bytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(port_bytes, port)
	key_bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(key_bytes, conn_key)
	return append(port_bytes, key_bytes...)
}

func decode_build_outbound(build_conn_bytes []byte) (uint16, uint32, error) {
	if len(build_conn_bytes) != 6 {
		return 0, 0, errors.New("decode_build_conn input error")
	}
	port := binary.LittleEndian.Uint16(build_conn_bytes[0:2])
	if port > 65535 {
		return port, 0, errors.New("port range err:" + strconv.Itoa(int(port)))
	}

	conn_key := binary.LittleEndian.Uint32(build_conn_bytes[2:6])

	return port, conn_key, nil
}

func encode_peerlist(plist []*Peer) []byte {
	//len uint16 : 2 bytes
	//peer: 4 bytes + 2 bytes (ip + port)

	result := []byte{}

	len_bytes := make([]byte, 2)
	if len(plist) == 0 {
		binary.LittleEndian.PutUint16(len_bytes, 0)
		return len_bytes
	}

	binary.LittleEndian.PutUint16(len_bytes, uint16(len(plist)))

	result = append(result, len_bytes...)

	for _, peer := range plist {

		if peer.Port > 65535 {
			continue
		}

		ipv4 := net.ParseIP(peer.Ip).To4()
		if ipv4 == nil {
			continue
		}

		octets := strings.Split(peer.Ip, ".")
		octet0, _ := strconv.Atoi(octets[0])
		octet1, _ := strconv.Atoi(octets[1])
		octet2, _ := strconv.Atoi(octets[2])
		octet3, _ := strconv.Atoi(octets[3])
		ip_bytes := []byte{byte(octet0), byte(octet1), byte(octet2), byte(octet3)}

		port_bytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(port_bytes, peer.Port)

		result = append(result, ip_bytes...)
		result = append(result, port_bytes...)
	}

	return result

}

func decode_peerlist(pl_bytes []byte) ([]*Peer, error) {
	if len(pl_bytes) < 2 {
		return nil, errors.New("decode_peerlist format error")
	}

	pl_len := binary.LittleEndian.Uint16(pl_bytes[0:2])
	if int(pl_len) > PEERLIST_LIMIT {
		return nil, errors.New("decode_peerlist overlimit:" + strconv.Itoa(PEERLIST_LIMIT))
	}

	if pl_len == 0 {
		return []*Peer{}, nil
	}

	if len(pl_bytes) != 2+int(pl_len)*6 {
		return nil, errors.New("decode_peerlist content length not match")
	}

	result := []*Peer{}

	fmt.Println("pl_bytes", pl_bytes)

	for i := 0; i < int(pl_len); i++ {
		start_pos := 2 + i*6
		ip := ""
		ip = ip + strconv.Itoa(int(pl_bytes[start_pos])) + "."
		ip = ip + strconv.Itoa(int(pl_bytes[start_pos+1])) + "."
		ip = ip + strconv.Itoa(int(pl_bytes[start_pos+2])) + "."
		ip = ip + strconv.Itoa(int(pl_bytes[start_pos+3])) + "."

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
