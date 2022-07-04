package p2p

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
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
	Bucket          map[uint32]map[uint16]*feeler_peer `json:"bucket"` //bucket num ,offset => peer
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

func (tm *TableManager) get_new_bucket_position(ip_split []string) uint32 {
	return (SHA256Uint32(ip_split[0]+"."+ip_split[1]) + tm.random_code) % NEW_TABLE_BUCKET_NUM
}

func (tm *TableManager) get_tried_bucket_position(ip_split []string) uint32 {
	return (SHA256Uint32(ip_split[0]+"."+ip_split[1]) + tm.random_code) % TRIED_TABLE_BUCKET_NUM
}

func (tm *TableManager) get_bucket_offset(ip_split []string) uint16 {
	return uint16((SHA256Uint32(ip_split[2]+"."+ip_split[3]) + tm.random_code) % uint32(BUCKET_SIZE))
}

//return peer list from tried table with non-overlap ip and
//the returned list size won't exceed the size_limit
func (tm *TableManager) get_peers_from_tried_table(size_limit int) []*Peer {
	tm.tried_table_lock.Lock()
	defer tm.tried_table_lock.Unlock()

	result := []*Peer{}
	for i := len(tm.tried_table_task) - 1; i >= 0; i-- {
		if len(result) >= size_limit {
			return result
		}

		result = append(result, &Peer{
			Ip:   strings.Join(tm.tried_table_task[i].Ip_split, "."),
			Port: tm.tried_table_task[i].Port,
		})
	}

	return result
}

func (tm *TableManager) add_peer_to_new_table(p *Peer) error {
	//check ipv4 format correct
	ip := net.ParseIP(p.Ip)
	if ip == nil || ip.To4() == nil {
		return errors.New("ip format error")
	}

	ip_split := strings.Split(p.Ip, ".")

	//check port format correct
	if p.Port == 0 || p.Port > 65535 {
		errors.New("port range [0,65535] error")
	}

	tm.new_table.Bucket[tm.get_new_bucket_position(ip_split)][tm.get_bucket_offset(ip_split)] = &feeler_peer{
		Ip_split:    ip_split,
		Port:        p.Port,
		Feeler_time: 0,
	}
	return nil
}

func (tm *TableManager) feel_new_table() {
	tm.new_table_lock.Lock()
	tm.tried_table_lock.Lock()
	defer tm.tried_table_lock.Unlock()
	defer tm.new_table_lock.Unlock()

	nt_bucket_keys := reflect.ValueOf(tm.new_table.Bucket).MapKeys()
	nt_bucket_count := len(nt_bucket_keys)
	if nt_bucket_count == 0 {
		return
	}

	nt_bucket_target_pos := nt_bucket_keys[rand.Intn(nt_bucket_count)].Interface().(uint32)
	nt_bucket_target := tm.new_table.Bucket[nt_bucket_target_pos]
	nt_target_peer_keys := reflect.ValueOf(nt_bucket_target).MapKeys()
	nt_peer_count := len(nt_target_peer_keys)
	if nt_peer_count == 0 {
		return
	}

	nt_target_peer := nt_bucket_target[nt_target_peer_keys[rand.Intn(nt_peer_count)].Interface().(uint16)]

	tt_bucket_pos := tm.get_tried_bucket_position(nt_target_peer.Ip_split)
	bucket_offset := tm.get_bucket_offset(nt_target_peer.Ip_split)

	//insert when no one on this slot
	if tm.tried_table.Bucket[tt_bucket_pos] == nil || tm.tried_table.Bucket[tt_bucket_pos][bucket_offset] == nil {
		ping_peer(&Peer{Ip: strings.Join(nt_target_peer.Ip_split[:], "."), Port: nt_target_peer.Port}, func(err error) {
			if err != nil {
				tm.logger.Debugln("ping peer error:", err)
			} else {
				f_p := &feeler_peer{
					Ip_split:    nt_target_peer.Ip_split,
					Port:        nt_target_peer.Port,
					Feeler_time: time.Now().Unix(),
				}

				tm.tried_table.Bucket[tt_bucket_pos][bucket_offset] = f_p
				tm.tried_table_task = append(tm.tried_table_task, f_p)
			}
		})
	}

	//delete related slots from new table
	delete(tm.new_table.Bucket[nt_bucket_target_pos], bucket_offset)
	if len(tm.new_table.Bucket[nt_bucket_target_pos]) == 0 {
		delete(tm.new_table.Bucket, nt_bucket_target_pos)
	}

}

func (tm *TableManager) feel_tried_table() {
	if len(tm.tried_table_task) == 0 {
		return
	}
	//pop a test
	tm.tried_table_lock.Lock()
	task := tm.tried_table_task[0]
	if len(tm.tried_table_task) > 1 {
		copy(tm.tried_table_task, tm.tried_table_task[1:])
		tm.tried_table_task = tm.tried_table_task[:len(tm.tried_table_task)-1]
	} else {
		tm.tried_table_task = []*feeler_peer{}
	}
	tm.tried_table_lock.Unlock()

	if time.Now().Unix()-task.Feeler_time < 1800 {
		//don't need feel again within 0.5 hour
		tm.tried_table_lock.Lock()
		tm.tried_table_task = append(tm.tried_table_task, task)
		tm.tried_table_lock.Unlock()
	} else {
		ping_peer(&Peer{Ip: strings.Join(task.Ip_split[:], "."), Port: task.Port}, func(err error) {
			tm.tried_table_lock.Lock()
			if err != nil {
				//delete related slots from tried table
				tt_bucket_pos := tm.get_tried_bucket_position(task.Ip_split)
				bucket_offset := tm.get_bucket_offset(task.Ip_split)

				delete(tm.tried_table.Bucket[tt_bucket_pos], bucket_offset)
				if len(tm.tried_table.Bucket[tt_bucket_pos]) == 0 {
					delete(tm.tried_table.Bucket, tt_bucket_pos)
				}

				tm.logger.Debugln("ping peer error:", err)
			} else {
				task.Feeler_time = time.Now().Unix()
				tm.tried_table_task = append(tm.tried_table_task, task)
			}
			tm.tried_table_lock.Unlock()
		})
	}
}

func deamon_save_tried_table(tm *TableManager) {
	for {
		//save every 15mins
		time.Sleep(15 * time.Minute)

		func(table_manager *TableManager) {
			table_manager.tried_table_lock.Lock()
			defer table_manager.tried_table_lock.Unlock()

			table_manager.tried_table.Update_unixtime = time.Now().Unix()
			tt_bytes, tt_err := json.Marshal(table_manager.tried_table)

			if tt_err == nil {
				tm.kvdb.Set(TRIED_TABLE, tt_bytes)
			} else {
				tm.logger.Errorln("deamon_save_tried_table save err", tt_err)
			}
		}(tm)
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
		if rand.Intn(2) == 1 {
			//feel new table
			tm.feel_new_table()
		} else {
			//feel tried table
			tm.feel_tried_table()
		}
		last_feeler_time = time.Now().Unix()
	}
}
