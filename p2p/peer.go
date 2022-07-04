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
	Hub            *Hub
	Peer           *Peer
	conn           *net.Conn
	rpc_client     *byte_rpc.Client
	close_callback func(*PeerConn, error)
}

func NewPeerConn(hub *Hub, peer *Peer, close_callback func(*PeerConn, error)) *PeerConn {
	return &PeerConn{
		Peer:           peer,
		close_callback: close_callback,
		Hub:            hub,
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
	endpoint := peerConn.Peer.Ip + ":" + strconv.FormatUint(uint64(peerConn.Peer.Port), 10)
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.Dial("tcp", endpoint)
	if err != nil {
		return errors.New("buildInboundConn err:" + endpoint)
	}

	peerConn.conn = &conn
	return nil
}

func (peerConn *PeerConn) Run() *PeerConn {

	peerConn.rpc_client = byte_rpc.NewClient(io.ReadWriteCloser(*peerConn.conn), &byte_rpc.Config{
		Version:             peerConn.Hub.config.P2p_version,
		Sub_version:         peerConn.Hub.config.P2p_sub_version,
		Body_max_bytes:      peerConn.Hub.config.P2p_body_max_bytes,
		Method_max_bytes:    peerConn.Hub.config.P2p_method_max_bytes,
		Live_check_duration: peerConn.Hub.config.P2p_live_check_duration,
		Conn_closed_callback: func(err error) {
			peerConn.close_callback(peerConn, err)
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

func (pc *PeerConn) reg_peerlist() *PeerConn {
	pc.rpc_client.Register(METHOD_PEERLIST, func(input []byte) []byte {

		pl := make(map[string]*Peer)

		pc.Hub.out_bound_peer_lock.Lock()
		for _, opc := range pc.Hub.out_bound_peer_conns {
			pl[opc.Peer.Ip] = opc.Peer
		}
		pc.Hub.out_bound_peer_lock.Unlock()

		pc.Hub.in_bound_peer_lock.Lock()
		for _, ipc := range pc.Hub.in_bound_peer_conns {
			pl[ipc.Peer.Ip] = ipc.Peer
		}
		pc.Hub.in_bound_peer_lock.Unlock()

		need_more := PEERLIST_LIMIT - len(pl)

		if need_more > 0 {

			for _, tt_p := range pc.Hub.table_manager.get_peers_from_tried_table(int(float32(need_more) * 0.7)) {
				pl[tt_p.Ip] = tt_p
			}

			for _, nt_p := range pc.Hub.table_manager.get_peers_from_new_table(int(float32(need_more) * 0.3)) {
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

func (pc *PeerConn) reg_ping() *PeerConn {
	pc.rpc_client.Register(METHOD_PING, func(input []byte) []byte {
		//change this to hub key to detect self connection
		return []byte(encode_ping(pc.Hub.id))
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

func (pc *PeerConn) reg_build_outbound() *PeerConn {

	pc.rpc_client.Register(METHOD_BUILD_INBOUND, func(input []byte) []byte {
		if !pc.Hub.is_outbound_target(pc.Peer.Ip) {
			time.AfterFunc(time.Second*1, func() { pc.Close() })
			return []byte(MSG_REJECTED)
		}

		//register all the handlers
		err := pc.RegisterRpcHandlers(pc.Hub.hanlder)
		if err != nil {
			time.AfterFunc(time.Second*1, func() { pc.Close() })
			pc.Hub.logger.Errorln("METHOD_BUILD_INBOUND RegisterRpcHandlers error", err)
			return []byte(MSG_REJECTED)
		}

		if len(pc.Hub.out_bound_peer_conns) > int(pc.Hub.config.P2p_outbound_limit) {
			time.AfterFunc(time.Second*1, func() { pc.Close() })
			pc.Hub.logger.Errorln("METHOD_BUILD_INBOUND overlimit")
			return []byte(MSG_REJECTED)
		}

		//////clear the old conn /////////////////////
		pc.Hub.out_bound_peer_lock.Lock()
		old_ob_p := pc.Hub.out_bound_peer_conns[pc.Peer.Ip]
		pc.Hub.out_bound_peer_conns[pc.Peer.Ip] = pc
		pc.Hub.out_bound_peer_lock.Unlock()
		////////////////////////////////

		//kick out the old stable conn
		if old_ob_p != nil {
			pc.Hub.logger.Debugln("METHOD_BUILD_INBOUND kick out old out_bound_conn")
			old_ob_p.Close()
		}

		old_ib_p := pc.Hub.in_bound_peer_conns[pc.Peer.Ip]
		if old_ib_p != nil {
			pc.Hub.logger.Debugln("METHOD_BUILD_OUTBOUND kick out old in_bound_conn")
			old_ib_p.Close()
		}

		return []byte(MSG_APPROVED)
	})

	return pc
}

func (pc *PeerConn) reg_build_inbound() *PeerConn {
	pc.rpc_client.Register(METHOD_BUILD_OUTBOUND, func(input []byte) []byte {

		port, err := decode_build_conn(input)
		if err != nil {
			return []byte(MSG_PORT_ERR)
		}

		pc.Peer.Port = port
		inbound_peer := NewPeerConn(pc.Hub, &Peer{
			Ip:   pc.Peer.Ip,
			Port: pc.Peer.Port,
		}, func(pc *PeerConn, err error) {
			if err != nil {
				pc.Hub.logger.Errorln("inbound conn close with error:", err)
			} else {
				pc.Hub.logger.Debugln("inbound conn close without error")
			}
			pc.Hub.in_bound_peer_lock.Lock()
			if pc.Hub.in_bound_peer_conns[pc.Peer.Ip] == pc {
				delete(pc.Hub.in_bound_peer_conns, pc.Peer.Ip)
			}
			(*pc.conn).Close()
			pc.Hub.in_bound_peer_lock.Unlock()
		})

		//register all the handlers
		err = inbound_peer.RegisterRpcHandlers(pc.Hub.hanlder)
		if err != nil {
			time.AfterFunc(time.Second*1, func() { inbound_peer.Close() })
			pc.Hub.logger.Errorln("METHOD_BUILD_OUTBOUND RegRpcHandlers error", err)
			return []byte(MSG_REJECTED)
		}

		if len(pc.Hub.in_bound_peer_conns) > int(pc.Hub.config.P2p_inbound_limit) {
			time.AfterFunc(time.Second*1, func() { inbound_peer.Close() })
			pc.Hub.logger.Errorln("METHOD_BUILD_OUTBOUND overlimit")
			return []byte(MSG_REJECTED)
		}

		//////clear the old conn /////////////////////
		pc.Hub.in_bound_peer_lock.Lock()
		old_ib_p := pc.Hub.in_bound_peer_conns[pc.Peer.Ip]
		pc.Hub.in_bound_peer_conns[pc.Peer.Ip] = inbound_peer
		pc.Hub.in_bound_peer_lock.Unlock()
		////////////////////////////////

		//kick out the old stable conn
		if old_ib_p != nil {
			pc.Hub.logger.Debugln("METHOD_BUILD_OUTBOUND kick out old in_bound_conn")
			old_ib_p.Close()
		}

		old_ob_p := pc.Hub.out_bound_peer_conns[pc.Peer.Ip]
		if old_ob_p != nil {
			pc.Hub.logger.Debugln("METHOD_BUILD_OUTBOUND kick out old out_bound_conn")
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
