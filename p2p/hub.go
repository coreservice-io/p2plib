package p2p

type Hub struct {
	KVDB          *KVDB
	InBoundPeers  []Peer
	OutBoundPeers []Peer
	InBoundLimit  uint
	OutBoundLimit uint
}
