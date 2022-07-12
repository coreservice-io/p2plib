package p2p

//const
const HUB_ID_LEN = 8
const PEERLIST_LIMIT = 1000 //max peers returned from remote peerlist request
const IP_CONN_LIMIT = 3     //the connection limit for the incoming ip 1+1+1 ,1 for stable conn , 1 for feeler ,1 for retry stable conn

//table
const TRIED_TABLE = "tried_table"
const BUCKET_SIZE = 64
const NEW_TABLE_BUCKET_NUM = 2048
const TRIED_TABLE_BUCKET_NUM = 64

const TRIED_TABLE_DBKV_SAVE_INTERVAL = 60   //seconds
const NEW_TABLE_BUFFER_UPDATE_INTERVAL = 60 //seconds

//peerlist and feeler
const FEELER_INTERVAL = 1            //feel some peer in new/tried table every {FEELER_INTERVAL} second
const REFRESH_PEERLIST_INTERVAL = 30 //seconds

type P2pConfig struct {
	P2p_version          uint16
	P2p_sub_version      uint16
	P2p_body_max_bytes   uint32
	P2p_method_max_bytes uint8
}

var p2p_config *P2pConfig

func Initialize(p2p_conf *P2pConfig) {
	p2p_config = p2p_conf
}
