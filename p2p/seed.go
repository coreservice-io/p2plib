package p2p

import (
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/coreservice-io/reference"
)

type Seed struct {
	Host string //either ip or domain name
	Port uint16
}

type SeedManager struct {
	//byte_rpc_conf *byte_rpc.Config
	seeds     []*Seed
	peer_pool []*Peer
	ref       *reference.Reference
}

func NewSeedManager(seeds []*Seed, ref *reference.Reference) *SeedManager {
	//remove ipv6 seeds which not supported
	filtered_seeds := []*Seed{}

	for _, s := range seeds {
		if s.Port > 65535 {
			continue
		}
		ipv4 := net.ParseIP(s.Host).To4()
		if ipv4 != nil {
			filtered_seeds = append(filtered_seeds, s)
		} else {
			ipv6 := net.ParseIP(s.Host).To16()
			if ipv6 != nil {
				//we don't support ipv6
				continue
			} else {
				//domain is supported
				filtered_seeds = append(filtered_seeds, s)
			}
		}
	}

	return &SeedManager{
		seeds:     filtered_seeds,
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
	temp_seed_peer := new_peer_conn(nil, 0, func(pc *PeerConn) {
		pc.closed = true
	}).set_conn(&conn).run()

	defer temp_seed_peer.close()

	rmsg, err := temp_seed_peer.send_msg(METHOD_PEERLIST, nil)
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

func (sm *SeedManager) pick_random_seed_peer() *Peer {
	if len(sm.seeds) == 0 {
		return nil
	}

	rand.Shuffle(len(sm.seeds), func(i, j int) { sm.seeds[i], sm.seeds[j] = sm.seeds[j], sm.seeds[i] })

	for _, seed := range sm.seeds {

		ipv4 := net.ParseIP(seed.Host).To4()
		if ipv4 != nil {
			return &Peer{
				Ip:   ipv4.String(),
				Port: seed.Port,
			}
		} else {

			ipv6 := net.ParseIP(seed.Host).To16()
			if ipv6 != nil {
				//we don't support ipv6
				continue
			}

			//it maybe some domain just try to
			//get the ipv4 of it
			ips, lookup_err := net.LookupIP(seed.Host)
			if lookup_err != nil {
				continue
			}

			for _, ip := range ips {
				if ipv4 := ip.To4(); ipv4 != nil {
					return &Peer{
						Ip:   ipv4.String(),
						Port: seed.Port,
					}
				}
			}
		}
	}

	return nil
}
