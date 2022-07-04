package example

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type P2p_kvdb struct {
	db *leveldb.DB
}

func NewP2pKVDB(levdb *leveldb.DB) *P2p_kvdb {
	return &P2p_kvdb{
		db: levdb,
	}
}

func (kvdb *P2p_kvdb) Set(key string, value []byte) error {
	return kvdb.db.Put([]byte(key), []byte(value), nil)
}

func (kvdb *P2p_kvdb) Delete(key string) error {
	return kvdb.db.Delete([]byte(key), nil)
}

func (kvdb *P2p_kvdb) Get(key string) ([]byte, error) {
	return kvdb.db.Get([]byte(key), nil)
}
