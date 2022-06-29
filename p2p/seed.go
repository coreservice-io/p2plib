package p2p

import (
	"math/rand"
)

type Seed struct {
	Host string //either ip or domain name
	Port uint16
}

type SeedManager struct {
	Seeds    []*Seed
	PeerPool []*Peer
}

func (sm *SeedManager) update_peer_pool(host string, port uint16) {

	pc := NewPeerConn(nil, true, &Peer{Ip: host, Port: port}, nil)
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

func (sm *SeedManager) SamplingPeersFromSeed() {
	if len(sm.Seeds) > 0 {
		pick_seed := sm.Seeds[rand.Intn(len(sm.Seeds))]
		sm.update_peer_pool(pick_seed.Host, pick_seed.Port)
	}
}

func (sm *SeedManager) SamplingPeersFromPeer() {
	if len(sm.PeerPool) > 0 {
		pick_peer := sm.PeerPool[rand.Intn(len(sm.PeerPool))]
		sm.update_peer_pool(pick_peer.Ip, pick_peer.Port)
	}
}
