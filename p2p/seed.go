package p2p

import (
	"errors"
	"math/rand"
	"net"

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
	ref      *reference.Reference
}

func (sm *SeedManager) get_ip(seed *Seed) (string, error) {

	if seed == nil || seed.Host == "" {
		return "", errors.New("seed host empty")
	}

	key := "seed_ip:" + seed.Host
	value, _ := sm.ref.Get(key)
	if value != nil {
		return *value.(*string), nil
	}

	ipvalue := IP_NOT_FOUND

	ips, _ := net.LookupIP(seed.Host)
	for _, ip := range ips {
		if ipv4 := ip.To4(); ipv4 != nil {
			ipvalue = ipv4.String()
		}
	}

	if ipvalue == IP_NOT_FOUND {
		sm.ref.Set(key, IP_NOT_FOUND, 300)
		return "", errors.New(IP_NOT_FOUND)
	} else {
		sm.ref.Set(key, IP_NOT_FOUND, 1800)
	}

	return ipvalue, nil
}

func (sm *SeedManager) get_peer() *Peer {
	if len(sm.PeerPool) > 0 {
		return sm.PeerPool[rand.Intn(len(sm.PeerPool))]
	}
	return nil
}

func (sm *SeedManager) update_peer_pool(host string, port uint16) {

	pc := NewPeerConn(nil, &Peer{Ip: host, Port: port}, nil)
	dial_err := pc.Dial()
	if dial_err != nil {
		return
	}

	rmsg, err := pc.SendMsg(METHOD_PEERLIST, nil)
	if err != nil {
		return
	}

	plist, pl_err := decode_peerlist(rmsg)
	if pl_err != nil {
		return
	}

	sm.PeerPool = append(plist, sm.PeerPool...)
	sm.PeerPool = sm.PeerPool[0:REMOTE_PEERLIST_LIMIT]
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
