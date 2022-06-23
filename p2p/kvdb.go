package p2p

type KVDB interface {
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	Get(key []byte) ([]byte, error)
}
