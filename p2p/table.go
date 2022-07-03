package p2p

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/coreservice-io/log"
)

func SHA256Uint32(input string) uint32 {
	bytes := sha256.Sum256([]byte(input))
	return binary.LittleEndian.Uint32(bytes[0:32])
}

type feeler_peer struct {
	Ip_split    []string `json:"ip_split"`
	Port        uint16   `json:"port"`
	Feeler_time int64    `json:"feeler_time"`
}

type table struct {
	Bucket          map[uint32]map[uint16]*feeler_peer `json:bucket` //bucket num ,offset => peer
	Update_unixtime int64                              `json:"update_unixtime"`
}

type TableManager struct {
	new_table_lock sync.Mutex
	new_table      *table

	tried_table_lock sync.Mutex
	tried_table      *table
	tried_table_task []*feeler_peer

	kvdb        KVDB
	logger      log.Logger
	random_code uint32
}

func (tm *TableManager) initialize() {
	rand.Seed(time.Now().UnixNano())

	//
	tm.random_code = rand.Uint32()

	//recover from kvdb
	tt := &table{}
	tt_bytes, tt_err := tm.kvdb.Get(TRIED_TABLE)
	if tt_err == nil {
		json_err := json.Unmarshal(tt_bytes, tt)
		if json_err == nil && tt.Update_unixtime > time.Now().Unix()-3600 {
			tm.tried_table = tt
		}
	}
}

// func (tm *TableManager) Reset() {

// 	//reset the random key
// 	tm.random_code = rand.Uint32()

// 	//reset tried table
// 	tm.tried_table.bucket = make(map[uint32]map[uint16]*feeler_peer)
// 	tt_bytes, tt_err := json.Marshal(tm.tried_table)
// 	if tt_err == nil {
// 		tm.kvdb.Set(TRIED_TABLE, tt_bytes)
// 	} else {
// 		tm.logger.Errorln("ResetTriedTable err", tt_err)
// 	}

// 	//reset new table
// 	tm.new_table.bucket = make(map[uint32]map[uint16]*feeler_peer)

// }

func (tm *TableManager) get_new_bucket_num(ip_split []string) uint32 {
	return (SHA256Uint32(ip_split[0]+"."+ip_split[1]) + tm.random_code) % NEW_TABLE_BUCKET_NUM
}

func (tm *TableManager) get_tried_bucket_num(ip_split []string) uint32 {
	return (SHA256Uint32(ip_split[0]+"."+ip_split[1]) + tm.random_code) % TRIED_TABLE_BUCKET_NUM
}

func (tm *TableManager) get_bucket_offset(ip_split []string) uint16 {
	return uint16((SHA256Uint32(ip_split[2]+"."+ip_split[3]) + tm.random_code) % uint32(BUCKET_SIZE))
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

	tm.new_table.Bucket[tm.get_new_bucket_num(ip_split)][tm.get_bucket_offset(ip_split)] = &feeler_peer{
		Ip_split:    ip_split,
		Port:        p.Port,
		Feeler_time: 0,
	}

}

func (tm *TableManager) new_table_feeler() {
	tm.new_table_lock.Lock()
	tm.tried_table_lock.Lock()
	defer tm.tried_table_lock.Unlock()
	defer tm.new_table_lock.Unlock()

	new_buckets_keys := reflect.ValueOf(tm.new_table.Bucket).MapKeys()
	new_buckets_num := len(new_buckets_keys)
	if new_buckets_num == 0 {
		return
	}

	new_bucket_target_key := new_buckets_keys[rand.Intn(new_buckets_num)].Interface().(uint32)
	new_bucket_target := tm.new_table.Bucket[new_bucket_target_key]
	new_target_peer_keys := reflect.ValueOf(new_bucket_target).MapKeys()
	new_peer_num := len(new_target_peer_keys)
	if new_peer_num == 0 {
		return
	}

	new_target_feeler := new_bucket_target[new_target_peer_keys[rand.Intn(new_peer_num)].Interface().(uint16)]

	tried_bucket_num := tm.get_tried_bucket_num(new_target_feeler.Ip_split)
	tried_bucket_offset := tm.get_bucket_offset(new_target_feeler.Ip_split)

	//insert when no one on this slot
	if tm.tried_table.Bucket[tried_bucket_num] == nil || tm.tried_table.Bucket[tried_bucket_num][tried_bucket_offset] == nil {
		tm.tried_table.Bucket[tried_bucket_num][tried_bucket_offset] = new_target_feeler
		//push the task
		tm.tried_table_task = append(tm.tried_table_task, new_target_feeler)
	}

	//delete related slots from new table
	delete(tm.new_table.Bucket[new_bucket_target_key], tried_bucket_offset)
	if new_peer_num == 1 {
		delete(tm.new_table.Bucket, new_bucket_target_key)
	}

}

func (tm *TableManager) tried_table_feeler() {
	if len(tm.tried_table_task) == 0 {
		return
	}

	//pop a test
	tm.new_table_lock.Lock()
	task := tm.tried_table_task[0]
	if len(tm.tried_table_task) > 1 {
		copy(tm.tried_table_task, tm.tried_table_task[1:])
		tm.tried_table_task = tm.tried_table_task[:len(tm.tried_table_task)-1]
	} else {
		tm.tried_table_task = []*feeler_peer{}
	}
	tm.new_table_lock.Unlock()

	ping_peer(&Peer{Ip: strings.Join(task.Ip_split[:], "."), Port: task.Port}, func(err error) {
		if err != nil {
			tm.logger.Debugln("ping peer error:", err)
		} else {
			tm.new_table_lock.Lock()
			tm.tried_table_task = append(tm.tried_table_task, &feeler_peer{
				Ip_split:    task.Ip_split,
				Port:        task.Port,
				Feeler_time: time.Now().Unix(),
			})
			tm.new_table_lock.Unlock()
		}
	})

}

func random_bool() bool {
	return rand.Intn(2) == 1
}

func deamon_save_tried_table(tm *TableManager) {
	for {
		//save every 15mins
		time.Sleep(15 * time.Minute)
		tm.tried_table_lock.Lock()
		tm.tried_table.Update_unixtime = time.Now().Unix()
		tt_bytes, tt_err := json.Marshal(tm.tried_table)
		tm.tried_table_lock.Unlock()
		if tt_err == nil {
			tm.kvdb.Set(TRIED_TABLE, tt_bytes)
		} else {
			tm.logger.Errorln("deamon_save_tried_table save err", tt_err)
		}
	}
}

func deamon_feeler_connection(tm *TableManager) {

	last_feeler_time := time.Now().Unix()

	for {
		/////////////////////////////////
		if time.Now().Unix()-last_feeler_time < FEELER_INTERVAL {
			time.Sleep(time.Second * FEELER_INTERVAL)
			continue
		}
		/////////////////////////////////

		if random_bool() {
			//feeler new table
			tm.new_table_feeler()
		} else {
			//feeler tried table
			tm.tried_table_feeler()
		}

		last_feeler_time = time.Now().Unix()
	}
}
