package p2p

import (
	"errors"
	"time"

	"github.com/coreservice-io/byte_rpc"
)

//return the hub_id,error of the other end
func ping_peer(p *Peer) (uint64, error) {

	pc := new_peer_conn(nil, &Peer{
		Ip:   p.Ip,
		Port: p.Port,
	}, 0, nil)

	err := pc.dail_with_timeout(5 * time.Second)
	if err != nil {
		pc.close()
		return 0, err
	}

	defer func() {
		pc.send_msg(METHOD_CLOSE, nil)
		pc.close()
	}()

	pc.run()

	pr, perr := pc.send_msg(METHOD_PING, nil)
	if perr != nil {
		return 0, perr
	}

	return decode_ping(pr)
}

func request_build_outbound_conn(hub *Hub, peer *Peer) error {

	if hub.out_bound_peer_conns[peer.Ip] != nil || hub.in_bound_peer_conns[peer.Ip] != nil {
		hub.logger.Debugln("request_build_outbound_conn with ip exist in conns already,ip", peer.Ip)
		return nil
	}

	hub.set_outbound_target(peer.Ip)

	port_bytes := encode_build_conn(peer.Port)

	outbound_peer := new_peer_conn(&byte_rpc.Config{
		Version:              hub.config.P2p_version,
		Sub_version:          hub.config.P2p_sub_version,
		Body_max_bytes:       hub.config.P2p_body_max_bytes,
		Method_max_bytes:     hub.config.P2p_method_max_bytes,
		Conn_closed_callback: nil,
	}, &Peer{
		Ip:   peer.Ip,
		Port: peer.Port,
	}, 0, nil)

	err := outbound_peer.dial()
	if err != nil {
		hub.logger.Errorln(err)
		outbound_peer.close()
		return err
	}
	outbound_peer.run()

	defer func() {
		outbound_peer.send_msg(METHOD_CLOSE, nil)
		outbound_peer.close()
	}()

	pr, perr := outbound_peer.send_msg(METHOD_PING, nil)
	if perr != nil {
		return perr
	}

	peer_hub_id, peer_hub_id_err := decode_ping(pr)
	if peer_hub_id_err != nil {
		return errors.New("ping result error")
	}

	if peer_hub_id != hub.id {
		_, err = outbound_peer.send_msg(METHOD_BUILD_OUTBOUND, port_bytes)
		if err != nil {
			return err
		}
	}

	return nil
}

//set the outbound conns to kvdb
func update_kvdb_outbound_conns(hub *Hub) {
	hub.out_bound_peer_lock.Lock()
	defer hub.out_bound_peer_lock.Unlock()

	plist := []*Peer{}
	for _, out_pc := range hub.out_bound_peer_conns {
		plist = append(plist, &Peer{
			Ip:   out_pc.peer.Ip,
			Port: out_pc.peer.Port,
		})
	}
	kvdb_set_outbounds(hub.kvdb, plist)
}

//retrieve from remote peers and update local peerlist
func deamon_refresh_peerlist(hub *Hub) {

	for {

		all_pcs := []*PeerConn{}
		/////////////////////////////
		hub.in_bound_peer_lock.Lock()
		hub.out_bound_peer_lock.Lock()

		for _, pc := range hub.in_bound_peer_conns {
			all_pcs = append(all_pcs, pc)
		}

		for _, pc := range hub.out_bound_peer_conns {
			all_pcs = append(all_pcs, pc)
		}

		hub.out_bound_peer_lock.Unlock()
		hub.in_bound_peer_lock.Unlock()
		/////////////////////////////

		for _, pc := range all_pcs {

			pl, pl_err := pc.send_msg(METHOD_PEERLIST, nil)
			if pl_err != nil {
				hub.logger.Debugln("METHOD_PEERLIST err", pl_err)
				continue
			}

			peer_list, err := decode_peerlist(pl)
			if err != nil {
				hub.logger.Debugln("decode_peerlist err", peer_list)
				continue
			}

			if len(peer_list) == 0 {
				continue
			}

			hub.table_manager.add_peers_to_new_table(peer_list)
			break
		}

		time.Sleep(300 * time.Second)
	}

}

//keep outbound connections exist
func deamon_keep_outbounds(hub *Hub) {

	for {

		if len(hub.out_bound_peer_conns) >= int(hub.config.P2p_outbound_limit) {
			hub.logger.Infoln("outbound reach limit")
			time.Sleep(30 * time.Second)
			continue
		}

		//if some connection break (maybe caused by feeler connection )
		//try to reconnect the old connection
		if len(hub.out_bound_peer_conns) == 0 {
			hub.logger.Infoln("try rebuild last outbound connections from dbkv ")

			plist, err := kvdb_get_outbounds(hub.kvdb)
			if err != nil {
				hub.logger.Warnln("kvdb_get_outbounds warning:", err)
			} else {
				if len(plist) > 0 {
					for _, peer := range plist {
						request_build_outbound_conn(hub, peer)
					}
					kvdb_set_outbounds(hub.kvdb, []*Peer{}) //reset kvdb to prevent re-dial
					time.Sleep(30 * time.Second)
				}
			}
		}

		//try connect to peers with the help of seed
		if len(hub.out_bound_peer_conns) == 0 {
			hub.seed_manager.sampling_peers_from_seed()
			for t := 0; t < 3; t++ {
				hub.seed_manager.sampling_peers_from_peer()
			}

			for j := 0; j < int(hub.config.P2p_outbound_limit); j++ {
				s_p := hub.seed_manager.get_random_peer()
				if s_p != nil && hub.out_bound_peer_conns[s_p.Ip] == nil && hub.in_bound_peer_conns[s_p.Ip] == nil {
					request_build_outbound_conn(hub, &Peer{Ip: s_p.Ip, Port: s_p.Port})
				}
			}
			time.Sleep(60 * time.Second)
		}

		//try directly connect to seed
		if len(hub.out_bound_peer_conns) == 0 {
			sp := hub.seed_manager.pick_random_seed_peer()
			if sp != nil {
				request_build_outbound_conn(hub, sp)
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

			//allergic sleep
			for i := 900; i >= 0; i-- {
				time.Sleep(1 * time.Second)
				if len(hub.out_bound_peer_conns) == 0 {
					break
				}
			}
		}

		update_kvdb_outbound_conns(hub)

	}
}
