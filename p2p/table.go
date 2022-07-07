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
	new_table_lock   sync.Mutex
	new_table        *table
	new_table_buffer []*Peer // a limited-size fifo buffer used for search

	tried_table_lock sync.Mutex
	tried_table      *table
	tried_table_task []*feeler_peer

	kvdb        KVDB
	logger      log.Logger
	random_code uint32
}

func new_table_manager(kvdb KVDB, logger log.Logger) (*TableManager, error) {

	if kvdb == nil {
		return nil, errors.New("kvdb empty error")
	}

	if logger == nil {
		return nil, errors.New("logger empty error")
	}

	return &TableManager{
		new_table: &table{
			Bucket:          map[uint32]map[uint16]*feeler_peer{},
			Update_unixtime: 0,
		},
		tried_table: &table{
			Bucket:          map[uint32]map[uint16]*feeler_peer{},
			Update_unixtime: 0,
		},
		new_table_buffer: []*Peer{},
		tried_table_task: []*feeler_peer{},
		kvdb:             kvdb,
		logger:           logger,
		random_code:      0,
	}, nil
}

func (tm *TableManager) initialize() {
	rand.Seed(time.Now().UnixNano())
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

func (tm *TableManager) get_peers_from_new_table(size_limit int) []*Peer {
	tm.new_table_lock.Lock()
	defer tm.new_table_lock.Unlock()
	result := []*Peer{}

	for i, p := range tm.new_table_buffer {
		if i >= size_limit {
			break
		}
		result = append(result, &Peer{
			Ip:   p.Ip,
			Port: p.Port,
		})
	}
	return result
}

func (tm *TableManager) add_peers_to_new_table(pl []*Peer) {

	tm.new_table_lock.Lock()
	defer tm.new_table_lock.Unlock()

	for _, peer := range pl {
		//check ipv4 format correct
		ip := net.ParseIP(peer.Ip)
		if ip == nil || ip.To4() == nil {
			continue
		}

		ip_split := strings.Split(peer.Ip, ".")
		//check port format correct
		if peer.Port == 0 || peer.Port > 65535 {
			continue
		}

		bucket_p := tm.get_new_bucket_position(ip_split)
		bucket_offset := tm.get_bucket_offset(ip_split)

		if tm.new_table.Bucket[bucket_p][bucket_offset] != nil {
			old_ip := strings.Join(tm.new_table.Bucket[bucket_p][bucket_offset].Ip_split, ".")
			if peer.Ip == old_ip {
				continue
			}
		}

		tm.new_table.Bucket[bucket_p][bucket_offset] = &feeler_peer{
			Ip_split:    ip_split,
			Port:        peer.Port,
			Feeler_time: 0,
		}

		tm.new_table_buffer = append(tm.new_table_buffer, &Peer{
			Ip:   peer.Ip,
			Port: peer.Port,
		})
	}

}

func (tm *TableManager) feel_new_table_rand_target() *feeler_peer {

	tm.new_table_lock.Lock()
	tm.tried_table_lock.Lock()

	defer tm.tried_table_lock.Unlock()
	defer tm.new_table_lock.Unlock()

	nt_bucket_keys := reflect.ValueOf(tm.new_table.Bucket).MapKeys()
	nt_bucket_count := len(nt_bucket_keys)
	if nt_bucket_count == 0 {
		return nil
	}

	nt_bucket_target_pos := nt_bucket_keys[rand.Intn(nt_bucket_count)].Interface().(uint32)
	nt_bucket_target := tm.new_table.Bucket[nt_bucket_target_pos]
	nt_target_peer_keys := reflect.ValueOf(nt_bucket_target).MapKeys()
	nt_peer_count := len(nt_target_peer_keys)
	if nt_peer_count == 0 {
		delete(tm.new_table.Bucket, nt_bucket_target_pos)
		return nil
	}

	nt_target_peer := nt_bucket_target[nt_target_peer_keys[rand.Intn(nt_peer_count)].Interface().(uint16)]
	tt_bucket_pos := tm.get_tried_bucket_position(nt_target_peer.Ip_split)
	bucket_offset := tm.get_bucket_offset(nt_target_peer.Ip_split)

	//delete related slots from new table
	delete(tm.new_table.Bucket[nt_bucket_target_pos], bucket_offset)
	if len(tm.new_table.Bucket[nt_bucket_target_pos]) == 0 {
		delete(tm.new_table.Bucket, nt_bucket_target_pos)
	}

	if tm.tried_table.Bucket[tt_bucket_pos] != nil && tm.tried_table.Bucket[tt_bucket_pos][bucket_offset] != nil {
		//already exist then just pass
		return nil
	}

	return nt_target_peer
}

func (tm *TableManager) feel_new_table() {

	///////////////////////////
	var feeler_target *feeler_peer
	for i := 0; i < 10; i++ {
		feeler_target = tm.feel_new_table_rand_target()
		if feeler_target != nil {
			break
		}
	}
	if feeler_target == nil {
		return
	}
	///////////////////////////
	_, p_err := ping_peer(&Peer{Ip: strings.Join(feeler_target.Ip_split[:], "."), Port: feeler_target.Port})
	if p_err != nil {
		tm.logger.Debugln("ping peer error:", p_err)
		return
	}
	/////////////////////////////
	tm.tried_table_lock.Lock()
	f_p := &feeler_peer{
		Ip_split:    feeler_target.Ip_split,
		Port:        feeler_target.Port,
		Feeler_time: time.Now().Unix(),
	}

	tt_bucket_pos := tm.get_tried_bucket_position(f_p.Ip_split)
	bucket_offset := tm.get_bucket_offset(f_p.Ip_split)
	tm.tried_table.Bucket[tt_bucket_pos][bucket_offset] = f_p
	tm.tried_table_task = append(tm.tried_table_task, f_p)
	tm.tried_table_lock.Unlock()
	/////////////////////////////
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
		return
	}

	_, ping_err := ping_peer(&Peer{Ip: strings.Join(task.Ip_split[:], "."), Port: task.Port})
	///////////////////////////
	tm.tried_table_lock.Lock()
	if ping_err != nil {
		//delete related slots from tried table
		tt_bucket_pos := tm.get_tried_bucket_position(task.Ip_split)
		bucket_offset := tm.get_bucket_offset(task.Ip_split)

		delete(tm.tried_table.Bucket[tt_bucket_pos], bucket_offset)
		if len(tm.tried_table.Bucket[tt_bucket_pos]) == 0 {
			delete(tm.tried_table.Bucket, tt_bucket_pos)
		}
		tm.logger.Debugln("ping peer error:", ping_err)
	} else {
		task.Feeler_time = time.Now().Unix()
		tm.tried_table_task = append(tm.tried_table_task, task)
	}
	tm.tried_table_lock.Unlock()
	//////////////////////////////
}

func deamon_save_kvdb_tried_table(tm *TableManager) {
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

//feeler connections
func deamon_feeler(tm *TableManager) {
	for {
		/////////////////////////////////
		time.Sleep(time.Second * FEELER_INTERVAL)
		/////////////////////////////////
		if rand.Intn(2) == 1 {
			//feel new table
			tm.feel_new_table()
		} else {
			//feel tried table
			tm.feel_tried_table()
		}
	}
}

func deamon_update_new_table_buffer(tm *TableManager) {

	for {
		tm.new_table_lock.Lock()

		counter := 0
		nt_bucket_keys := reflect.ValueOf(tm.new_table.Bucket).MapKeys()
		rand.Shuffle(len(nt_bucket_keys), func(i, j int) { nt_bucket_keys[i], nt_bucket_keys[j] = nt_bucket_keys[j], nt_bucket_keys[i] })

		for _, bk := range nt_bucket_keys {
			for _, peer := range tm.new_table.Bucket[bk.Interface().(uint32)] {
				tm.new_table_buffer = append(tm.new_table_buffer, &Peer{
					Ip:   strings.Join(peer.Ip_split, "."),
					Port: peer.Port,
				})
				counter++
			}

			if counter >= PEERLIST_LIMIT {
				break
			}
		}

		if len(tm.new_table_buffer) > PEERLIST_LIMIT {
			tm.new_table_buffer = tm.new_table_buffer[0:PEERLIST_LIMIT]
		}

		tm.new_table_lock.Unlock()

		//every 60 seconds do the job
		time.Sleep(time.Second * 60)

	}

}
