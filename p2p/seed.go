package p2p

import (
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/coreservice-io/reference"
)

const IP_NOT_FOUND = "ip_not_found"

type Seed struct {
	Host string //either ip or domain name
	Port uint16
}

type SeedManager struct {
	Seeds    []*Seed
	PeerPool []*Peer
	Ref      *reference.Reference
}

func (sm *SeedManager) get_random_peer() *Peer {
	if len(sm.PeerPool) > 0 {
		return sm.PeerPool[rand.Intn(len(sm.PeerPool))]
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
	rmsg, err := NewPeerConn(nil, nil, nil).SetConn(&conn).SendMsg(METHOD_PEERLIST, nil)
	if err != nil {
		return
	}

	plist, pl_err := decode_peerlist(rmsg)
	if pl_err != nil {
		return
	}

	sm.PeerPool = append(plist, sm.PeerPool...)
	if len(sm.PeerPool) > PEERLIST_LIMIT {
		sm.PeerPool = sm.PeerPool[0:PEERLIST_LIMIT]
	}

}

func (sm *SeedManager) sampling_peers_from_seed() {
	if len(sm.Seeds) > 0 {
		pick_seed := sm.Seeds[rand.Intn(len(sm.Seeds))]
		sm.update_peer_pool(pick_seed.Host, pick_seed.Port)
	}
}

func (sm *SeedManager) sampling_peers_from_peer() {
	if len(sm.PeerPool) > 0 {
		pick_peer := sm.PeerPool[rand.Intn(len(sm.PeerPool))]
		sm.update_peer_pool(pick_peer.Ip, pick_peer.Port)
	}
}
