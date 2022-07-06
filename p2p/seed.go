package p2p

import (
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/coreservice-io/byte_rpc"
	"github.com/coreservice-io/reference"
)

const IP_NOT_FOUND = "ip_not_found"

type Seed struct {
	Host string //either ip or domain name
	Port uint16
}

type SeedManager struct {
	byte_rpc_conf *byte_rpc.Config
	seeds         []*Seed
	peer_pool     []*Peer
	ref           *reference.Reference
}

func NewSeedManager(seeds []*Seed, ref *reference.Reference) *SeedManager {
	return &SeedManager{
		seeds:     seeds,
		ref:       ref,
		peer_pool: []*Peer{},
	}
}

func (sm *SeedManager) get_random_peer() *Peer {
	if len(sm.peer_pool) > 0 {
		return sm.peer_pool[rand.Intn(len(sm.peer_pool))]
	}
	return nil
}

func (sm *SeedManager) update_peer_pool(host string, port uint16) {

	///////////////
	endpoint := host + ":" + strconv.FormatUint(uint64(port), 10)
	dialer := net.Dialer{Timeout: 15 * time.Second}
	conn, err := dialer.Dial("tcp", endpoint)
	if err != nil {
		return
	}
	///////////////
	rmsg, err := new_peer_conn(sm.byte_rpc_conf, nil, 0, nil).set_conn(&conn).run().send_msg(METHOD_PEERLIST, nil)
	if err != nil {
		return
	}

	plist, pl_err := decode_peerlist(rmsg)
	if pl_err != nil {
		return
	}

	sm.peer_pool = append(plist, sm.peer_pool...)
	if len(sm.peer_pool) > PEERLIST_LIMIT {
		sm.peer_pool = sm.peer_pool[0:PEERLIST_LIMIT]
	}

}

func (sm *SeedManager) sampling_peers_from_seed() {
	if len(sm.seeds) > 0 {
		pick_seed := sm.seeds[rand.Intn(len(sm.seeds))]
		sm.update_peer_pool(pick_seed.Host, pick_seed.Port)
	}
}

func (sm *SeedManager) sampling_peers_from_peer() {
	if len(sm.peer_pool) > 0 {
		pick_peer := sm.peer_pool[rand.Intn(len(sm.peer_pool))]
		sm.update_peer_pool(pick_peer.Ip, pick_peer.Port)
	}
}
