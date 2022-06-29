package p2p

type Seed struct {
	Host string //either ip or domain name
	Port int
}

type SeedManager struct {
	Seeds    []Seed
	PeerPool []Peer
}

func (sm *SeedManager) Bootstrap() {
	//1.randomly pick the seed and get peerlist into the peerpool

	//2.pick the peer from the peerpool and get peerlist into the peerpool x n times

	//

}

func (sm *SeedManager) PopPeers() []*Peer {

	return nil
}
