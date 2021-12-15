package ytsync

import (
	proto "github.com/golang/protobuf/proto"
	ytab "github.com/yottachain/yotta-arraybase"
	pb "github.com/yottachain/yotta-sync-server/pb"
)

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
	//BlockDelTab blocks_tab table
	BlockDelTab = "blocks_bak"
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
	ID  int64  `bson:"_id" json:"_id"`
	VHP []byte `bson:"VHP" json:"VHP"`
	VHB []byte `bson:"VHB" json:"VHB"`
	KED []byte `bson:"KED" json:"KED"`
	VNF int32  `bson:"VNF" json:"VNF"`
	AR  int32  `bson:"AR" json:"AR"`
	//SnID   int32    `bson:"snId" json:"-"`
	Shards []*Shard `bson:"-" json:"shards"`
}

//Shard struct
type Shard struct {
	ID      int64  `bson:"_id" json:"_id"`
	NodeID  int32  `bson:"nodeId" json:"nid"`
	VHF     []byte `bson:"VHF" json:"VHF"`
	BlockID int64  `bson:"blockid" json:"bid"`
	NodeID2 int32  `bson:"nodeId2" json:"nid2"`
}

//ShardRebuildMeta struct
type ShardRebuildMeta struct {
	ID  int64 `bson:"_id" json:"_id"`
	VFI int64 `bson:"VFI" json:"VFI"`
	NID int32 `bson:"nid" json:"nid"`
	SID int32 `bson:"sid" json:"sid"`
}

//BlockDel struct
type BlockDel struct {
	ID  int64 `bson:"_id" json:"_id"`
	VBI int64 `bson:"VBI" json:"VBI"`
}

//DataResp response data of sync server
type DataResp struct {
	SNID   int      `json:"SN"`
	Blocks []*Block `json:"blocks"`
	//Shards   []*Shard            `json:"shards"`
	Rebuilds []*ShardRebuildMeta `json:"rebuilds"`
	BlockDel []*BlockDel         `json:"deletes"`
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

func (block *Block) ConvertToAB() *ytab.Block {
	blockAB := new(ytab.Block)
	blockAB.ID = uint64(block.ID)
	if len(block.VHP) == 32 {
		blockAB.VHP = block.VHP
	} else {
		blockAB.VHP = make([]byte, 32)
	}
	if len(block.VHB) == 16 {
		blockAB.VHB = block.VHB
	} else {
		blockAB.VHB = make([]byte, 16)
	}
	if len(block.KED) == 32 {
		blockAB.KED = block.KED
	} else {
		blockAB.KED = make([]byte, 32)
	}
	blockAB.VNF = uint8(block.VNF)
	blockAB.AR = int16(block.AR)
	for _, s := range block.Shards {
		vhf := s.VHF
		if len(vhf) != 16 {
			vhf = make([]byte, 16)
		}
		blockAB.Shards = append(blockAB.Shards, &ytab.Shard{VHF: vhf, NodeID: uint32(s.NodeID), NodeID2: uint32(s.NodeID2)})
	}
	return blockAB
}

// Convert convert Block strcut to BlockMsg
func (block *Block) Convert() *pb.BlockMsg {
	msg := &pb.BlockMsg{
		Id:  block.ID,
		Vhp: block.VHP,
		Vhb: block.VHB,
		Ked: block.KED,
		Vnf: block.VNF,
		Ar:  block.AR,
		//SnID: block.SnID,
	}
	for _, s := range block.Shards {
		msg.Shards = append(msg.Shards, s.Convert())
	}
	return msg
}

// Fillby convert BlockMsg to Block struct
func (block *Block) Fillby(msg *pb.BlockMsg) {
	block.ID = msg.Id
	block.VHP = msg.Vhp
	block.VHB = msg.Vhb
	block.KED = msg.Ked
	block.VNF = msg.Vnf
	block.AR = msg.Ar
	//block.SnID = msg.SnID
	for _, s := range msg.Shards {
		shard := new(Shard)
		shard.Fillby(s)
		block.Shards = append(block.Shards, shard)
	}
}

// FillBytes convert bytes to Block strcut
func (block *Block) FillBytes(buf []byte) error {
	blockMsg := new(pb.BlockMsg)
	err := proto.Unmarshal(buf, blockMsg)
	if err != nil {
		return err
	}
	block.Fillby(blockMsg)
	return nil
}

// ConvertBytes convert Block struct to bytes
func (block *Block) ConvertBytes() ([]byte, error) {
	return proto.Marshal(block.Convert())
}

// Convert convert Shard strcut to ShardMsg
func (shard *Shard) Convert() *pb.ShardMsg {
	return &pb.ShardMsg{
		Id:     shard.ID,
		NodeID: shard.NodeID,
		Vhf:    shard.VHF,
		//BlockID:   shard.BlockID,
		NodeID2: shard.NodeID2,
	}
}

// Fillby convert ShardMsg to Shard struct
func (shard *Shard) Fillby(msg *pb.ShardMsg) {
	shard.ID = msg.Id
	shard.NodeID = msg.NodeID
	shard.VHF = msg.Vhf
	//shard.BlockID = msg.BlockID
	shard.NodeID2 = msg.NodeID2
}

// FillBytes convert bytes to Shard strcut
func (shard *Shard) FillBytes(buf []byte) error {
	shardMsg := new(pb.ShardMsg)
	err := proto.Unmarshal(buf, shardMsg)
	if err != nil {
		return err
	}
	shard.Fillby(shardMsg)
	return nil
}

// ConvertBytes convert Shard struct to bytes
func (shard *Shard) ConvertBytes() ([]byte, error) {
	return proto.Marshal(shard.Convert())
}

// Convert convert Checkpoint strcut to CheckPointMsg
func (checkpoint *CheckPoint) Convert() *pb.CheckPointMsg {
	return &pb.CheckPointMsg{
		Id:        checkpoint.ID,
		Start:     checkpoint.Start,
		Timestamp: checkpoint.Timestamp,
	}
}

// Fillby convert CheckPointMsg to CheckPoint struct
func (checkpoint *CheckPoint) Fillby(msg *pb.CheckPointMsg) {
	checkpoint.ID = msg.Id
	checkpoint.Start = msg.Start
	checkpoint.Timestamp = msg.Timestamp
}

// FillBytes convert bytes to CheckPoint strcut
func (checkpoint *CheckPoint) FillBytes(buf []byte) error {
	checkpointMsg := new(pb.CheckPointMsg)
	err := proto.Unmarshal(buf, checkpointMsg)
	if err != nil {
		return err
	}
	checkpoint.Fillby(checkpointMsg)
	return nil
}

// ConvertBytes convert CheckPoint struct to bytes
func (checkpoint *CheckPoint) ConvertBytes() ([]byte, error) {
	return proto.Marshal(checkpoint.Convert())
}
