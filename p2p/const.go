package p2p

//const
const REMOTE_PEERLIST_LIMIT = 1000 //max peers returned from remote peerlist request
const IP_CONN_LIMIT = 3            //the connection limit for the incoming ip 1+1+1 ,1 for stable conn , 1 for feeler ,1 for retry stable conn

const BUCKET_SIZE = 64
const NEW_TABLE_BUCKET_NUM = 2048
const TRIED_TABLE_BUCKET_NUM = 64
