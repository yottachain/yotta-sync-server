package controllers

//Block 数据块id及分片数量
type Block struct {
	ID  int64 `bson:"_id"`
	VNF int32 `bson:"VNF"`
	AR  int32 `bson:"AR"`
}

//Shard 分片信息
type Shard struct {
	ID     int64 `bson:"_id"`
	NodeID int32 `bson:"nodeId"`
	VHF    byte  `bson:"VHF"`
}

type Msg struct {
	ID     int64 `bson:"_id"`
	VNF    int32 `bson:"VNF"`
	AR     int32 `bson:"AR"`
	Shards []Shard
}
