package main

import (
	"fmt"
	"time"

	"github.com/coreservice-io/logrus_log"
	"github.com/coreservice-io/p2plib/example"
	"github.com/coreservice-io/p2plib/p2p"
	"github.com/coreservice-io/reference"
	"github.com/syndtr/goleveldb/leveldb"
)

func main() {

	p2p_hub_conf := p2p.HubConfig{
		Hub_port:                8081,
		P2p_version:             1,
		P2p_sub_version:         1,
		P2p_body_max_bytes:      1024 * 1024,
		P2p_method_max_bytes:    32,
		P2p_live_check_duration: 60 * time.Second,
		P2p_inbound_limit:       128,
		P2p_outbound_limit:      8,
		Conn_pool_limit:         256,
	}

	levdb, err := leveldb.OpenFile("./leveldb", nil)
	if err != nil {
		fmt.Println("leveldb err", err)
		return
	}

	logger, llerr := logrus_log.New("./log", 2, 20, 30)
	if llerr != nil {
		fmt.Println("llerr ", llerr)
		return
	}

	kvdb := example.NewP2pKVDB(levdb)
	ref := reference.New()

	hub, hub_err := p2p.NewHub(kvdb, ref, make(map[string]bool),
		&p2p.SeedManager{Seeds: []*p2p.Seed{}, PeerPool: []*p2p.Peer{}, Ref: ref},
		&p2p_hub_conf, logger)
	if hub_err != nil {
		fmt.Println("hub_err", hub_err)
		return
	}

	hub.Start()

	fmt.Println("this is peer1")

	for {
		//never quit
		time.Sleep(time.Duration(1) * time.Hour)
	}

}
