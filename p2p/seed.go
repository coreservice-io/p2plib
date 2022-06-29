package p2p

import (
	"errors"
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

func (sm *SeedManager) update_peers_from_seeds(seeds_limit int) {

	pick_seed := sm.Seeds[rand.Intn(len(sm.Seeds))]

	pc := NewPeerConn(nil, true, &Peer{Ip: pick_seed.Host, Port: pick_seed.Port}, nil)
	dial_err := pc.Dial()
	if dial_err != nil {
		return
	}

	rmsg, err := pc.SendMsg(METHOD_PEERLIST, nil)
	if err != nil {
		return
	}

	plist, pl_err := decode_peerlist(seeds_limit, rmsg)
	if pl_err != nil {
		return
	}

	sm.PeerPool = append(sm.PeerPool, plist...)

}

func (sm *SeedManager) SamplingPeer() (*Peer, error) {

	if len(sm.PeerPool) == 0 {
		if len(sm.Seeds) == 0 {
			return nil, errors.New("seeds empty")
		}

	}
	return nil, nil
}
