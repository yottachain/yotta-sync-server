package controllers

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

//Block 数据块id及分片数量
type Block struct {
	ID  int64 `bson:"_id"`
	VNF int32 `bson:"VNF"`
	AR  int32 `bson:"AR"`
}

//Shard 分片信息
type Shard struct {
	ID      int64  `bson:"_id"`
	NodeID  int32  `bson:"nodeId"`
	VHF     []byte `bson:"VHF"`
	BlockID int64  `bson:"blockId"`
}

//WriteShard 用于写入到数据库
type WriteShard struct {
	ID      int64            `bson:"_id"`
	NodeID  int32            `bson:"nodeId"`
	VHF     primitive.Binary `bson:"VHF"`
	BlockID int64            `bson:"blockId"`
}

//Messages 返回消息结构体
type Messages struct {
	Blocks []Block
	Shards []Shard
}

//Msg 定义消息结构体
type Msg struct {
	ID     int64 `bson:"_id"`
	VNF    int32 `bson:"VNF"`
	AR     int32 `bson:"AR"`
	Shards []Shard
}

// func (msg *Msg) MarshalJSON() ([]byte, error) {
// 	return bson.Marshal(msg)
// }

// func (msg *Msg) UnmarshalJSON(b []byte) error {
// 	err := bson.Unmarshal(b, msg)
// 	if err != nil {
// 		return err
// 	}
// 	for i := 0; i < len(msg.Shards); i++ {
// 		msg.Shards[i].BlockID = msg.ID
// 	}
// 	return nil
// }
