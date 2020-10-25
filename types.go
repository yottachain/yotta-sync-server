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
)

//Block struct
type Block struct {
	ID   int64 `bson:"_id" json:"_id"`
	VNF  int32 `bson:"VNF" json:"VNF"`
	AR   int32 `bson:"AR" json:"AR"`
	SnID int   `bson:"snId" json:"-"`
}

//Shard struct
type Shard struct {
	ID      int64  `bson:"_id" json:"_id"`
	NodeID  int32  `bson:"nodeId" json:"nid"`
	VHF     []byte `bson:"VHF" json:"VHF"`
	BlockID int64  `bson:"blockid" json:"bid"`
}

//ShardRebuildMeta struct
type ShardRebuildMeta struct {
	ID  int64 `bson:"_id" json:"_id"`
	VFI int64 `bson:"VFI" json:"VFI"`
	NID int32 `bson:"nid" json:"nid"`
	SID int32 `bson:"sid" json:"sid"`
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
	ID        int32 `bson:"_id"`
	Start     int64 `bson:"start"`
	Timestamp int64 `bson:"timestamp"`
}

//Record struct
type Record struct {
	StartTime int32 `bson:"start"`
	EndTime   int32 `bson:"end"`
	Sn        int32 `bson:"sn"`
}
