package p2p

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/coreservice-io/log"
)

func SHA256Uint32(input string) uint32 {
	bytes := sha256.Sum256([]byte(input))
	return binary.LittleEndian.Uint32(bytes[0:32])
}

const TRIED_TABLE = "tried_table"

type Table struct {
	Bucket map[uint32]map[uint16]*Peer `json:bucket` //bucketnum ,offset => peer
}

type TableManager struct {
	new_table               *Table
	new_table_bucket_num    uint32
	new_table_bucket_size   uint16
	tried_table             *Table
	tried_table_bucket_num  uint32
	tried_table_bucket_size uint16
	kvdb                    KVDB
	logger                  log.Logger
	random_code             uint32
}

func (tm *TableManager) Reset() {

	//reset the random key
	tm.random_code = rand.Uint32()

	//reset tried table
	tm.tried_table.Bucket = make(map[uint32]map[uint16]*Peer)
	tt_bytes, tt_err := json.Marshal(tm.tried_table)
	if tt_err == nil {
		tm.kvdb.Set(TRIED_TABLE, tt_bytes)
	} else {
		tm.logger.Errorln("ResetTriedTable err", tt_err)
	}

	//reset new table
	tm.new_table.Bucket = make(map[uint32]map[uint16]*Peer)

}

func (tm *TableManager) AddPeerToNewTable(p *Peer) {
	//check ipv4 format correct
	ip := net.ParseIP(p.Ip)
	if ip == nil || ip.To4() == nil {
		return
	}

	ip_split := strings.Split(p.Ip, ".")

	//check port format correct
	if p.Port == 0 || p.Port > 65535 {
		return
	}

	ip_bucket_offset := (SHA256Uint32(ip_split[0]+"."+ip_split[1]) + tm.random_code) % tm.new_table_bucket_num
	ip_bucket_internal_offset := (SHA256Uint32(ip_split[2]+"."+ip_split[3]) + tm.random_code) % uint32(tm.new_table_bucket_size)

	tm.new_table.Bucket[ip_bucket_offset][uint16(ip_bucket_internal_offset)] = p

}

func (tm *TableManager) deamon_save_tried_table() {

	for {
		//save every 15mins
		time.Sleep(15 * time.Minute)

		tt_bytes, tt_err := json.Marshal(tm.tried_table)
		if tt_err == nil {
			tm.kvdb.Set(TRIED_TABLE, tt_bytes)
		} else {
			tm.logger.Errorln("deamon_save_tried_table save err", tt_err)
		}
	}
}

// func (pc *PeerConn) ping_peer(p *Peer, cb func(error)) {
// 	pr, perr := pc.SendMsg(METHOD_PING, nil)
// 	if perr != nil {
// 		cb(perr)
// 		return
// 	}
// 	if string(pr) != MSG_PONG {
// 		cb(errors.New("ping result error"))
// 		return
// 	}
// 	cb(nil)
// }

func (tm *TableManager) deamon_fill_tried_table() {

}
