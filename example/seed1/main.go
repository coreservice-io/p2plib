package main

import (
	"fmt"
	"time"

	"github.com/coreservice-io/log"
	"github.com/coreservice-io/logrus_log"
	"github.com/coreservice-io/p2plib/example"
	"github.com/coreservice-io/p2plib/p2p"
	"github.com/coreservice-io/reference"
	"github.com/syndtr/goleveldb/leveldb"
)

func main() {

	///////////////////////////////////////////////
	p2p.Initialize(&p2p.P2pConfig{
		P2p_version:          1,
		P2p_sub_version:      1,
		P2p_body_max_bytes:   1024 * 1024,
		P2p_method_max_bytes: 32,
	})

	p2p_hub_conf := p2p.HubConfig{
		Port:            8081,
		Heart_beat_secs: 60,
		Inbound_limit:   128,
		Outbound_limit:  8,
		Conns_limit:     256,
	}

	///////////////////////////////////////////////
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

	logger.SetLevel(log.DebugLevel)
	///////////////////////////////////////////////

	kvdb := example.NewP2pKVDB(levdb)
	ref := reference.New()
	ip_black_list := make(map[string]bool)

	///////////////////////////////////////////////

	hub, hub_err := p2p.NewHub(kvdb, ref, ip_black_list, p2p.NewSeedManager([]*p2p.Seed{
		{Host: "172.20.7.118", Port: 8081},
	}, ref), &p2p_hub_conf, logger)
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
