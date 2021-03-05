package ytsync

const (
	//Function tag
	Function = "function"
	//SNID tag
	SNID = "snID"
	//MinerID tag
	MinerID = "minerID"
	//BlockID tag
	BlockID = "blockID"
	//ShardID tag
	ShardID = "shardID"
)

const (
	//BlocksTab blocks table
	BlocksTab = "blocks"
	//ShardsTab shards table
	ShardsTab = "shards"
	//ShardsRebuildTab shards_rebuild table
	ShardsRebuildTab = "shards_rebuild"
	//CheckPointTab CheckPoint table
	CheckPointTab = "CheckPoint"
	//RecordTab record table
	RecordTab = "record"
	//CorruptBlockTab block_corrupt table
	CorruptBlockTab = "block_corrupt"
	//NodeLogTab NodeLog table
	NodeLogTab = "NodeLog"
)

//Block struct
type Block struct {
	ID   int64 `bson:"_id" json:"_id" db:"id"`
	VNF  int32 `bson:"VNF" json:"VNF" db:"vnf"`
	AR   int32 `bson:"AR" json:"AR" db:"ar"`
	SnID int   `bson:"snId" json:"-" db:"snid"`
}

//Shard struct
type Shard struct {
	ID      int64  `bson:"_id" json:"_id" db:"id"`
	NodeID  int32  `bson:"nodeId" json:"nid" db:"nid"`
	VHF     []byte `bson:"VHF" json:"VHF" db:"vhf"`
	BlockID int64  `bson:"blockid" json:"bid" db:"bid"`
}

//ShardRebuildMeta struct
type ShardRebuildMeta struct {
	ID  int64 `bson:"_id" json:"_id" db:"id"`
	VFI int64 `bson:"VFI" json:"VFI" db:"vfi"`
	NID int32 `bson:"nid" json:"nid" db:"nid"`
	SID int32 `bson:"sid" json:"sid" db:"sid"`
}

//DataResp response data of sync server
type DataResp struct {
	SNID     int                 `json:"SN"`
	Blocks   []*Block            `json:"blocks"`
	Shards   []*Shard            `json:"shards"`
	Rebuilds []*ShardRebuildMeta `json:"rebuilds"`
	From     int64               `json:"from"`
	Next     int64               `json:"next"`
	Size     int64               `json:"size"`
	More     bool                `json:"more"`
}

//CheckPoint struct
type CheckPoint struct {
	ID        int32 `bson:"_id" db:"id"`
	Start     int64 `bson:"start" db:"start"`
	Timestamp int64 `bson:"timestamp" db:"timestamp"`
}

//Record struct
type Record struct {
	StartTime int32 `bson:"start"`
	EndTime   int32 `bson:"end"`
	Sn        int32 `bson:"sn"`
}

//NodeLog log of node operation
type NodeLog struct {
	ID         int64  `bson:"_id" json:"_id"`
	MinerID    int32  `bson:"minerID" json:"minerID"`
	FromStatus int32  `bson:"fromStatus" json:"fromStatus"`
	ToStatus   int32  `bson:"toStatus" json:"toStatus"`
	Type       string `bson:"type" json:"type"`
	Timestamp  int64  `bson:"timestamp" json:"timestamp"`
}
