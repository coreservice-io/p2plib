package p2p

import "encoding/json"

type KVDB interface {
	Set(key string, value []byte) error
	Delete(key string) error
	Get(key string) ([]byte, error)
}

func kvdb_set_outbounds(kvdb KVDB, pl []*Peer) error {
	key := "outbounds"
	pl_bytes, err := json.Marshal(&pl)
	if err != nil {
		return err
	}
	return kvdb.Set(key, pl_bytes)
}

func kvdb_get_outbounds(kvdb KVDB) ([]*Peer, error) {
	key := "outbounds"
	pl_bytes, err := kvdb.Get(key)
	if err != nil {
		return nil, err
	}

	pl := []*Peer{}
	err = json.Unmarshal(pl_bytes, &pl)
	if err != nil {
		return nil, err
	}

	return pl, nil
}
