package controllers

import (
	"encoding/json"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

//Block 数据块id及分片数量
// type Block struct {
// 	ID  int64 `bson:"_id"`
// 	VNF int32 `bson:"VNF"`
// 	AR  int32 `bson:"AR"`
// }

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

type Block struct {
	ID     int64         `bson:"_id" json:"_id"`
	VNF    int32         `bson:"VNF" json:"VNF"`
	AR     int32         `bson:"AR" json:"AR"`
	Shards []interface{} `bson:"shards,omitempty" json:"shards,omitempty"`
}

//Shard 分片信息
type Shard struct {
	ID      int64  `bson:"_id" json:"_id"`
	NodeID  int32  `bson:"nodeId" json:"nodeId"`
	VHF     []byte `bson:"VHF" json:"VHF"`
	BlockID int64  `bson:"blockid,omitempty" json:"blockid,omitempty"`
}

func (block *Block) UnmarshalJSON(b []byte) error {
	x := &struct {
		ID     int64         `bson:"_id" json:"_id"`
		VNF    int32         `bson:"VNF" json:"VNF"`
		AR     int32         `bson:"AR" json:"AR"`
		Shards []interface{} `bson:"shards,omitempty" json:"shards,omitempty"`
	}{}
	err := json.Unmarshal(b, x)
	if err != nil {
		return err
	}
	block.ID = x.ID
	block.VNF = x.VNF
	block.AR = x.AR
	block.Shards = x.Shards
	for i := 0; i < len(block.Shards); i++ {
		block.Shards[i].(map[string]interface{})["blockid"] = block.ID
	}
	return nil
}
