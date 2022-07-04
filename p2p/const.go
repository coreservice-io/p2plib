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
const FEELER_INTERVAL = 1 //feel some peer in new/tried table every {FEELER_INTERVAL} second
