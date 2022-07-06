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
	byte_rpc_conf  *byte_rpc.Config
	peer           *Peer
	conn           *net.Conn
	rpc_client     *byte_rpc.Client
	close_callback func(*PeerConn, error)
}

func NewPeerConn(byte_rpc_conf *byte_rpc.Config, peer *Peer, close_callback func(*PeerConn, error)) *PeerConn {
	return &PeerConn{
		byte_rpc_conf:  byte_rpc_conf,
		peer:           peer,
		close_callback: close_callback,
	}
}

func (peerConn *PeerConn) SetConn(conn *net.Conn) *PeerConn {
	peerConn.conn = conn
	return peerConn
}

func (peerConn *PeerConn) RegisterRpcHandlers(handlers map[string]func([]byte) []byte) error {

	if peerConn.rpc_client == nil {
		return errors.New("rpc_client nil")
	}

	for method_str, m_handler := range handlers {
		err := peerConn.rpc_client.Register(method_str, m_handler)
		if err != nil {
			return err
		}
	}
	return nil
}

func (peerConn *PeerConn) Dial() error {
	return peerConn.DialWithTimeOut(15 * time.Second)
}

func (peerConn *PeerConn) DialWithTimeOut(timeout time.Duration) error {
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

func (peerConn *PeerConn) Run() *PeerConn {

	//fmt.Println(" peerConn.Hub.config.P2p_version", peerConn.hub.config.P2p_version)
	//fmt.Println(" peerConn.Hub.config.P2p_sub_version", peerConn.hub.config.P2p_sub_version)

	// peerConn.rpc_client = byte_rpc.NewClient(io.ReadWriteCloser(*peerConn.conn), &byte_rpc.Config{
	// 	Version:             peerConn.hub.config.P2p_version,
	// 	Sub_version:         peerConn.hub.config.P2p_sub_version,
	// 	Body_max_bytes:      peerConn.hub.config.P2p_body_max_bytes,
	// 	Method_max_bytes:    peerConn.hub.config.P2p_method_max_bytes,
	// 	Live_check_duration: peerConn.hub.config.P2p_live_check_duration,
	// 	Conn_closed_callback: func(err error) {
	// 		peerConn.close_callback(peerConn, err)
	// 	},
	// })

	peerConn.rpc_client = byte_rpc.NewClient(io.ReadWriteCloser(*peerConn.conn), &byte_rpc.Config{
		Version:             peerConn.byte_rpc_conf.Version,
		Sub_version:         peerConn.byte_rpc_conf.Version,
		Body_max_bytes:      peerConn.byte_rpc_conf.Body_max_bytes,
		Method_max_bytes:    peerConn.byte_rpc_conf.Method_max_bytes,
		Live_check_duration: peerConn.byte_rpc_conf.Live_check_duration,
		Conn_closed_callback: func(err error) {
			if peerConn.close_callback != nil {
				peerConn.close_callback(peerConn, err)
			}
		},
	})

	peerConn.rpc_client.StartLivenessCheck().Run()
	return peerConn
}

func (pc *PeerConn) Close() {
	if pc.close_callback != nil {
		pc.close_callback(pc, nil)
	}
}

func (pc *PeerConn) SendMsg(method string, msg []byte) ([]byte, error) {
	result, err_code := pc.rpc_client.Call(method, msg)
	if err_code != 0 {
		return nil, errors.New(byte_rpc.GetErrMsgStr(uint(err_code)))
	}
	return *result, nil
}

func (pc *PeerConn) reg_peerlist(hub *Hub) *PeerConn {
	pc.rpc_client.Register(METHOD_PEERLIST, func(input []byte) []byte {

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
	pc.rpc_client.Register(METHOD_PING, func(input []byte) []byte {
		//change this to hub key to detect self connection
		return []byte(encode_ping(hub.id))
	})
	return pc
}

func (pc *PeerConn) reg_close() *PeerConn {
	pc.rpc_client.Register(METHOD_CLOSE, func(input []byte) []byte {
		defer pc.Close()
		return []byte(METHOD_CLOSE)
	})
	return pc
}

func (pc *PeerConn) reg_build_outbound(hub *Hub) *PeerConn {

	pc.rpc_client.Register(METHOD_BUILD_INBOUND, func(input []byte) []byte {
		if !hub.is_outbound_target(pc.peer.Ip) {
			time.AfterFunc(time.Second*1, func() { pc.Close() })
			return []byte(MSG_REJECTED)
		}

		//register all the handlers
		err := pc.RegisterRpcHandlers(hub.hanlder)
		if err != nil {
			time.AfterFunc(time.Second*1, func() { pc.Close() })
			hub.logger.Errorln("METHOD_BUILD_INBOUND RegisterRpcHandlers error", err)
			return []byte(MSG_REJECTED)
		}

		if len(hub.out_bound_peer_conns) > int(hub.config.P2p_outbound_limit) {
			time.AfterFunc(time.Second*1, func() { pc.Close() })
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
			old_ob_p.Close()
		}

		old_ib_p := hub.in_bound_peer_conns[pc.peer.Ip]
		if old_ib_p != nil {
			hub.logger.Debugln("METHOD_BUILD_OUTBOUND kick out old in_bound_conn")
			old_ib_p.Close()
		}

		return []byte(MSG_APPROVED)
	})

	return pc
}

func (pc *PeerConn) reg_build_inbound(hub *Hub) *PeerConn {
	pc.rpc_client.Register(METHOD_BUILD_OUTBOUND, func(input []byte) []byte {

		port, err := decode_build_conn(input)
		if err != nil {
			return []byte(MSG_PORT_ERR)
		}

		pc.peer.Port = port
		inbound_peer := NewPeerConn(pc.byte_rpc_conf, &Peer{
			Ip:   pc.peer.Ip,
			Port: pc.peer.Port,
		}, func(pc *PeerConn, err error) {
			if err != nil {
				hub.logger.Errorln("inbound conn close with error:", err)
			} else {
				hub.logger.Debugln("inbound conn close without error")
			}
			hub.in_bound_peer_lock.Lock()
			if hub.in_bound_peer_conns[pc.peer.Ip] == pc {
				delete(hub.in_bound_peer_conns, pc.peer.Ip)
			}
			(*pc.conn).Close()
			hub.in_bound_peer_lock.Unlock()
		})

		//register all the handlers
		err = inbound_peer.RegisterRpcHandlers(hub.hanlder)
		if err != nil {
			time.AfterFunc(time.Second*1, func() { inbound_peer.Close() })
			hub.logger.Errorln("METHOD_BUILD_OUTBOUND RegRpcHandlers error", err)
			return []byte(MSG_REJECTED)
		}

		if len(hub.in_bound_peer_conns) > int(hub.config.P2p_inbound_limit) {
			time.AfterFunc(time.Second*1, func() { inbound_peer.Close() })
			hub.logger.Errorln("METHOD_BUILD_OUTBOUND overlimit")
			return []byte(MSG_REJECTED)
		}

		//////clear the old conn /////////////////////
		hub.in_bound_peer_lock.Lock()
		old_ib_p := hub.in_bound_peer_conns[pc.peer.Ip]
		hub.in_bound_peer_conns[pc.peer.Ip] = inbound_peer
		hub.in_bound_peer_lock.Unlock()
		////////////////////////////////

		//kick out the old stable conn
		if old_ib_p != nil {
			hub.logger.Debugln("METHOD_BUILD_OUTBOUND kick out old in_bound_conn")
			old_ib_p.Close()
		}

		old_ob_p := hub.out_bound_peer_conns[pc.peer.Ip]
		if old_ob_p != nil {
			hub.logger.Debugln("METHOD_BUILD_OUTBOUND kick out old out_bound_conn")
			old_ob_p.Close()
		}

		go func() {
			/////////dail remote tcp////////
			dial_err := inbound_peer.Dial()
			if dial_err != nil {
				inbound_peer.Close()
			}
			////////////////////////////////
			inbound_peer.Run()
			inbound_peer.SendMsg(METHOD_BUILD_INBOUND, nil)
		}()

		return []byte(MSG_APPROVED)
	})
	return pc
}
