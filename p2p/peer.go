package p2p

import (
	"errors"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/coreservice-io/byte_rpc"
)

type Peer struct {
	Ip   string `json:"ip"`
	Port uint16 `json:"port"`
}

type PeerConn struct {
	//byte_rpc_conf        *byte_rpc.Config
	peer                 *Peer
	conn                 *net.Conn
	rpc_client           *byte_rpc.Client
	close_callback       func(*PeerConn)
	handlers             map[string]func([]byte) []byte //registered handlers
	heart_beat_duratioin time.Duration
}

func new_peer_conn(peer *Peer, heart_beat_duratioin time.Duration, close_callback func(*PeerConn)) *PeerConn {
	return &PeerConn{
		peer:                 peer,
		close_callback:       close_callback,
		handlers:             make(map[string]func([]byte) []byte),
		heart_beat_duratioin: heart_beat_duratioin,
	}
}

func (peerConn *PeerConn) set_conn(conn *net.Conn) *PeerConn {
	peerConn.conn = conn
	return peerConn
}

func (peerConn *PeerConn) register_handlers(handlers map[string]func([]byte) []byte) {
	for method_str, m_handler := range handlers {
		peerConn.handlers[method_str] = m_handler
	}
}

func (peerConn *PeerConn) register_handler(method_str string, m_handler func([]byte) []byte) {
	peerConn.handlers[method_str] = m_handler
}

func (peerConn *PeerConn) register_rpc_handlers(handlers map[string]func([]byte) []byte) {
	for method_name, method_func := range handlers {
		peerConn.rpc_client.Register(method_name, method_func)
	}
}

func (peerConn *PeerConn) start_heart_beat(check_interval time.Duration, closed_callback func(error)) {

	go func() {

		for {
			time.Sleep(check_interval)

			pr, perr := peerConn.send_msg(METHOD_PING, nil)
			if perr != nil {
				peerConn.close()
				closed_callback(perr)
				break
			}

			if len(pr) != 8 {
				closed_callback(errors.New("ping result error"))
				break
			}
		}

	}()
}

func (peerConn *PeerConn) dial() error {
	return peerConn.dail_with_timeout(15 * time.Second)
}

func (peerConn *PeerConn) dail_with_timeout(timeout time.Duration) error {
	if peerConn.conn != nil {
		return nil
	}
	endpoint := peerConn.peer.Ip + ":" + strconv.FormatUint(uint64(peerConn.peer.Port), 10)
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.Dial("tcp", endpoint)
	if err != nil {
		return errors.New("buildInboundConn err:" + endpoint)
	}

	peerConn.conn = &conn
	return nil
}

func (peerConn *PeerConn) run() *PeerConn {

	peerConn.rpc_client = byte_rpc.NewClient(io.ReadWriteCloser(*peerConn.conn), &byte_rpc.Config{
		Version:          p2p_config.P2p_version,
		Sub_version:      p2p_config.P2p_sub_version,
		Body_max_bytes:   p2p_config.P2p_body_max_bytes,
		Method_max_bytes: p2p_config.P2p_method_max_bytes,
		Conn_closed_callback: func() {
			if peerConn.close_callback != nil {
				peerConn.close_callback(peerConn)
			}
		},
	})

	peerConn.register_rpc_handlers(peerConn.handlers)

	peerConn.rpc_client.Run()
	return peerConn
}

func (pc *PeerConn) close() {
	if pc.rpc_client != nil {
		pc.rpc_client.Close()
	}
}

func (pc *PeerConn) send_msg(method string, msg []byte) ([]byte, error) {
	result, err_code := pc.rpc_client.Call(method, msg)
	if err_code != 0 {
		return nil, errors.New(byte_rpc.GetErrMsgStr(uint(err_code)))
	}
	return *result, nil
}

func (pc *PeerConn) reg_peerlist(hub *Hub) *PeerConn {
	pc.register_handler(METHOD_PEERLIST, func(input []byte) []byte {

		pl := make(map[string]*Peer)

		hub.out_bound_peer_lock.Lock()
		for _, opc := range hub.out_bound_peer_conns {
			pl[opc.peer.Ip] = opc.peer
		}
		hub.out_bound_peer_lock.Unlock()

		hub.in_bound_peer_lock.Lock()
		for _, ipc := range hub.in_bound_peer_conns {
			pl[ipc.peer.Ip] = ipc.peer
		}
		hub.in_bound_peer_lock.Unlock()

		need_more := PEERLIST_LIMIT - len(pl)

		if need_more > 0 {

			for _, tt_p := range hub.table_manager.get_peers_from_tried_table(int(float32(need_more) * 0.7)) {
				pl[tt_p.Ip] = tt_p
			}

			for _, nt_p := range hub.table_manager.get_peers_from_new_table(int(float32(need_more) * 0.3)) {
				pl[nt_p.Ip] = nt_p
			}
		}

		///////////////////////////////
		to_encode := []*Peer{}
		for _, p := range pl {
			to_encode = append(to_encode, p)
		}

		return encode_peerlist(to_encode)
	})
	return pc
}

func (pc *PeerConn) reg_ping(hub *Hub) *PeerConn {
	pc.register_handler(METHOD_PING, func(input []byte) []byte {
		//change this to hub key to detect self connection
		return []byte(encode_ping(hub.id))
	})
	return pc
}

func (pc *PeerConn) reg_close() *PeerConn {
	pc.register_handler(METHOD_CLOSE, func(input []byte) []byte {
		defer pc.close()
		return []byte(METHOD_CLOSE)
	})
	return pc
}

func (pc *PeerConn) reg_build_outbound(hub *Hub) *PeerConn {

	pc.register_handler(METHOD_BUILD_INBOUND, func(input []byte) []byte {
		if !hub.is_outbound_target(pc.peer.Ip) {
			time.AfterFunc(time.Second*1, func() { pc.close() })
			return []byte(MSG_REJECTED)
		}

		//register all the handlers using rpc directly as
		//the conn is already running
		pc.register_rpc_handlers(hub.handlers)

		if len(hub.out_bound_peer_conns) > int(hub.config.Outbound_limit) {
			time.AfterFunc(time.Second*1, func() { pc.close() })
			hub.logger.Errorln("METHOD_BUILD_INBOUND overlimit")
			return []byte(MSG_REJECTED)
		}

		//////clear the old conn /////////////////////
		hub.out_bound_peer_lock.Lock()
		old_ob_p := hub.out_bound_peer_conns[pc.peer.Ip]
		hub.out_bound_peer_conns[pc.peer.Ip] = pc
		hub.out_bound_peer_lock.Unlock()
		////////////////////////////////

		//kick out the old stable conn
		if old_ob_p != nil {
			hub.logger.Debugln("METHOD_BUILD_INBOUND kick out old out_bound_conn")
			old_ob_p.close()
		}

		old_ib_p := hub.in_bound_peer_conns[pc.peer.Ip]
		if old_ib_p != nil {
			hub.logger.Debugln("METHOD_BUILD_OUTBOUND kick out old in_bound_conn")
			old_ib_p.close()
		}

		///////////////////////
		pc.start_heart_beat(pc.heart_beat_duratioin, func(err error) {
			if err != nil {
				hub.logger.Errorln("heart_beat error inside METHOD_BUILD_INBOUND", err)
			}
			hub.logger.Debugln("heart_beat inside METHOD_BUILD_INBOUND closed")
		})

		return []byte(MSG_APPROVED)
	})

	return pc
}

func (pc *PeerConn) reg_build_inbound(hub *Hub) *PeerConn {
	pc.register_handler(METHOD_BUILD_OUTBOUND, func(input []byte) []byte {

		port, err := decode_build_conn(input)
		if err != nil {
			return []byte(MSG_PORT_ERR)
		}

		pc.peer.Port = port
		inbound_peer := new_peer_conn(&Peer{
			Ip:   pc.peer.Ip,
			Port: pc.peer.Port,
		}, pc.heart_beat_duratioin, func(pc *PeerConn) {
			if err != nil {
				hub.logger.Errorln("METHOD_BUILD_OUTBOUND conn close with error:", err)
			} else {
				hub.logger.Debugln("METHOD_BUILD_OUTBOUND conn close without error")
			}
			hub.in_bound_peer_lock.Lock()
			if hub.in_bound_peer_conns[pc.peer.Ip] == pc {
				delete(hub.in_bound_peer_conns, pc.peer.Ip)
			}
			(*pc.conn).Close()
			hub.in_bound_peer_lock.Unlock()
		})

		if len(hub.in_bound_peer_conns) > int(hub.config.Inbound_limit) {
			time.AfterFunc(time.Second*1, func() { inbound_peer.close() })
			hub.logger.Errorln("METHOD_BUILD_OUTBOUND overlimit")
			return []byte(MSG_REJECTED)
		}

		//register all the handlers
		inbound_peer.reg_close().reg_ping(hub).reg_peerlist(hub)
		inbound_peer.register_handlers(hub.handlers)

		//////clear the old conn /////////////////////
		hub.in_bound_peer_lock.Lock()
		old_ib_p := hub.in_bound_peer_conns[pc.peer.Ip]
		hub.in_bound_peer_conns[pc.peer.Ip] = inbound_peer
		hub.in_bound_peer_lock.Unlock()
		////////////////////////////////

		//kick out the old stable conn
		if old_ib_p != nil {
			hub.logger.Debugln("METHOD_BUILD_OUTBOUND kick out old in_bound_conn")
			old_ib_p.close()
		}

		old_ob_p := hub.out_bound_peer_conns[pc.peer.Ip]
		if old_ob_p != nil {
			hub.logger.Debugln("METHOD_BUILD_OUTBOUND kick out old out_bound_conn")
			old_ob_p.close()
		}

		go func() {
			/////////dail remote tcp////////
			dial_err := inbound_peer.dial()
			if dial_err != nil {
				inbound_peer.close()
			}
			////////////////////////////////
			bi_r, bi_err := inbound_peer.send_msg(METHOD_BUILD_INBOUND, nil)
			if bi_err == nil && string(bi_r) == MSG_APPROVED {
				inbound_peer.run().start_heart_beat(inbound_peer.heart_beat_duratioin, func(err error) {
					if err != nil {
						hub.logger.Errorln("heart_beat error inside METHOD_BUILD_OUTBOUND", err)
					}
					hub.logger.Debugln("heart_beat  inside METHOD_BUILD_OUTBOUND closed")
				})

			} else {
				hub.logger.Errorln("METHOD_BUILD_INBOUND error:", bi_err, "result:", bi_r)
				inbound_peer.close()
			}

		}()

		return []byte(MSG_APPROVED)
	})
	return pc
}
