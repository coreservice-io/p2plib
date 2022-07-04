package p2p

import (
	"errors"
	"time"
)

func ping_peer(p *Peer, cb func(error)) {

	pc := NewPeerConn(nil, &Peer{
		Ip:   p.Ip,
		Port: p.Port,
	}, nil)

	err := pc.DialWithTimeOut(5 * time.Second)
	if err != nil {
		cb(err)
		return
	}

	pr, perr := pc.SendMsg(METHOD_PING, nil)
	if perr != nil {
		cb(perr)
		return
	}
	if string(pr) != MSG_PONG {
		cb(errors.New("ping result error"))
		return
	}

	pc.SendMsg(METHOD_CLOSE, nil)
	cb(nil)
}

func request_build_outbound_conn(hub *Hub, peer *Peer) error {

	if hub.out_bound_peer_conns[peer.Ip] != nil || hub.in_bound_peer_conns[peer.Ip] != nil {
		hub.logger.Debugln("request_build_outbound_conn with ip exist in conns already,ip", peer.Ip)
		return nil
	}

	hub.out_bound_peer_lock.Lock()
	defer hub.out_bound_peer_lock.Unlock()

	hub.set_outbound_target(peer.Ip)

	port_bytes, err := encode_build_conn(peer.Port)
	if err != nil {
		return err
	}

	outbound_peer := NewPeerConn(nil, &Peer{
		Ip:   peer.Ip,
		Port: peer.Port,
	}, nil)

	err = outbound_peer.Dial()
	if err != nil {
		return err
	}

	_, err = outbound_peer.SendMsg(METHOD_BUILD_OUTBOUND, port_bytes)
	if err != nil {
		return err
	}
	outbound_peer.SendMsg(METHOD_CLOSE, nil)
	outbound_peer.Close()
	return nil
}

//periodically set the outbound conns to dbkv
func deamon_update_outbound_conns(hub *Hub) {
	time.Sleep(300 * time.Second)
	hub.out_bound_peer_lock.Lock()
	defer hub.out_bound_peer_lock.Unlock()

	plist := []*Peer{}
	for _, out_pc := range hub.out_bound_peer_conns {
		plist = append(plist, &Peer{
			Ip:   out_pc.Peer.Ip,
			Port: out_pc.Peer.Port,
		})
	}
	kvdb_set_outbounds(*hub.kvdb, plist)
}

func deamon_keep_outbound_conns(hub *Hub) {

	for {
		if len(hub.out_bound_peer_conns) >= int(hub.config.P2p_outbound_limit) {
			hub.logger.Infoln("outbound reach limit")
			time.Sleep(30 * time.Second)
			continue
		}

		//todo if some connection break (maybe caused by feeler connection )
		//try to reconnect the old connection
		if len(hub.out_bound_peer_conns) == 0 {
			hub.logger.Infoln("try rebuild last outbound connections from dbkv ")

			plist, err := kvdb_get_outbounds(*hub.kvdb)
			if err != nil {
				hub.logger.Errorln("rebuild_last_outbound_conns err:", err)
			} else {
				if len(plist) > 0 {
					for _, peer := range plist {
						request_build_outbound_conn(hub, peer)
					}
					kvdb_set_outbounds(*hub.kvdb, []*Peer{}) //reset kvdb to prevent re-dial
					time.Sleep(30 * time.Second)
				}
			}
		}

		if len(hub.out_bound_peer_conns) == 0 {
			hub.seed_manager.sampling_peers_from_seed()
			for t := 0; t < 3; t++ {
				hub.seed_manager.sampling_peers_from_peer()
			}

			for j := 0; j < int(hub.config.P2p_outbound_limit); j++ {
				s_p := hub.seed_manager.get_peer()
				if s_p != nil && hub.out_bound_peer_conns[s_p.Ip] == nil {
					request_build_outbound_conn(hub, &Peer{Ip: s_p.Ip, Port: s_p.Port})
				}
			}
			time.Sleep(60 * time.Second)
		}

		//[eclipse attack] never pick from tried table when no outbound connection established yet
		if len(hub.out_bound_peer_conns) != 0 {
			//pick connection from tried table
			tt_peers := hub.table_manager.get_peers_from_tried_table(int(hub.config.P2p_outbound_limit))
			for _, t_p := range tt_peers {
				request_build_outbound_conn(hub, &Peer{Ip: t_p.Ip, Port: t_p.Port})
			}
			for i := 900; i >= 0; i-- {
				time.Sleep(1 * time.Second)
				if len(hub.out_bound_peer_conns) == 0 {
					break
				}
			}
		}
	}
}
