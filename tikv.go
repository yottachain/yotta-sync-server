package ytsync

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
	"github.com/yottachain/yotta-sync-server/pb"
)

const (
	PFX_BLOCKS     = "blocks"
	PFX_SHARDS     = "shards"
	PFX_SHARDNODES = "snodes"
	PFX_CHECKPOINT = "checkpoint"
)

var NoValError = errors.New("value not found")

//TikvDao dao struct for tikv operation
type TikvDao struct {
	cli *rawkv.Client
}

//NewDao create a new tikv DAO
func NewTikvDao(ctx context.Context, urls []string) (*TikvDao, error) {
	entry := log.WithFields(log.Fields{Function: "NewDao"})
	cli, err := rawkv.NewClient(ctx, urls, config.Default())
	if err != nil {
		entry.Errorf("create DAO failed: %v", urls)
		return nil, err
	}
	return &TikvDao{cli: cli}, nil
}

//FindCheckPoint find checkpoint record by SN ID
func (tikvDao *TikvDao) FindCheckPoint(ctx context.Context, id int32) (*CheckPoint, error) {
	entry := log.WithFields(log.Fields{Function: "FindCheckPoint", SNID: id})
	checkPoint := new(CheckPoint)
	buf, err := tikvDao.cli.Get(ctx, []byte(checkpointKey(id)))
	if err != nil {
		entry.WithError(err).Error("find checkpoint")
		return nil, err
	}
	if buf == nil {
		return nil, NoValError
	}
	checkPoint.FillBytes(buf)
	return checkPoint, nil
}

//InsertCheckPoint insert check point record
func (tikvDao *TikvDao) InsertCheckPoint(ctx context.Context, checkPoint *CheckPoint) error {
	entry := log.WithFields(log.Fields{Function: "InsertCheckPoint", SNID: checkPoint.ID})
	buf, err := checkPoint.ConvertBytes()
	if err != nil {
		entry.WithError(err).Errorf("convert checkpoint struct error: start->%d, timestamp->%d", checkPoint.Start, checkPoint.Timestamp)
		return err
	}
	err = tikvDao.cli.Put(ctx, []byte(checkpointKey(checkPoint.ID)), buf)
	if err != nil {
		entry.WithError(err).Errorf("insert error: start->%d, timestamp->%d", checkPoint.Start, checkPoint.Timestamp)
		return err
	}
	return nil
}

//InsertBlocks insert blocks into tikv
func (tikvDao *TikvDao) InsertBlocks(ctx context.Context, blocks []*Block) error {
	entry := log.WithFields(log.Fields{Function: "InsertBlocks"})
	if len(blocks) == 0 {
		return nil
	}
	keys := make([][]byte, 0)
	vals := make([][]byte, 0)
	for _, b := range blocks {
		buf, err := b.ConvertBytes()
		if err != nil {
			entry.WithError(err).Errorf("convert error: %v", b)
			return err
		}
		keys = append(keys, []byte(blocksKey(b.ID)))
		vals = append(vals, buf)
	}
	err := tikvDao.cli.BatchPut(ctx, keys, vals)
	if err != nil {
		entry.WithError(err).Error("batch put error")
		return err
	}
	return nil
}

//InsertShards insert shards into tikv
func (tikvDao *TikvDao) InsertShards(ctx context.Context, shards []*Shard) error {
	entry := log.WithFields(log.Fields{Function: "InsertShards"})
	if len(shards) == 0 {
		return nil
	}
	//keys1 := make([][]byte, 0)
	keys2 := make([][]byte, 0)
	keys3 := make([][]byte, 0)
	vals1 := make([][]byte, 0)
	vals2 := make([][]byte, 0)
	for _, s := range shards {
		buf, err := s.ConvertBytes()
		if err != nil {
			entry.WithError(err).Errorf("convert error: %v", s)
			return err
		}
		//keys1 = append(keys1, []byte(shardsKey(s.ID)))
		keys2 = append(keys2, []byte(shardsNodeKey(s.ID, s.NodeID)))
		vals1 = append(vals1, buf)
		if s.NodeID2 > 0 {
			keys3 = append(keys3, []byte(shardsNodeKey(s.ID, s.NodeID2)))
			vals2 = append(vals2, buf)
		}
	}
	// err := tikvDao.cli.BatchPut(ctx, keys1, vals1)
	// if err != nil {
	// 	entry.WithError(err).Error("batch put error: 1")
	// 	return err
	// }
	err := tikvDao.cli.BatchPut(ctx, keys2, vals1)
	if err != nil {
		entry.WithError(err).Error("batch put error: 1")
		return err
	}
	err = tikvDao.cli.BatchPut(ctx, keys3, vals2)
	if err != nil {
		entry.WithError(err).Error("batch put error: 2")
		return err
	}
	return nil
}

//UpdateShards update shards meta
func (tikvDao *TikvDao) UpdateShards(ctx context.Context, metas []*ShardRebuildMeta) error {
	entry := log.WithFields(log.Fields{Function: "UpdateShards"})
	if len(metas) == 0 {
		return nil
	}
	keys1 := make([][]byte, 0)
	for _, m := range metas {
		keys1 = append(keys1, []byte(shardsNodeKey(m.VFI, m.SID)))
	}
	bufs1, err := tikvDao.cli.BatchGet(ctx, keys1)
	if err != nil {
		entry.WithError(err).Error("batch get node shards failed")
		return err
	}
	nskeys1 := make([][]byte, 0)
	nskeys2 := make([][]byte, 0)
	nskeysdel := make([][]byte, 0)
	nsbufs1 := make([][]byte, 0)
	nsbufs2 := make([][]byte, 0)
	bkeysb := make([][]byte, 0)
	bshards := make([]*pb.ShardMsg, 0)
	for i := 0; i < len(bufs1); i++ {
		m := metas[i]
		b := bufs1[i]
		if b == nil {
			continue
		}
		msg := new(pb.ShardMsg)
		err := proto.Unmarshal(b, msg)
		if err != nil {
			entry.WithError(err).Error("unmarshal failed")
			return err
		}

		if msg.Timestamp > (m.ID>>32)+60 {
			continue
		}
		if m.SID == msg.NodeID && msg.NodeID2 == 0 {
			nskeys1 = append(nskeys1, []byte(shardsNodeKey(m.VFI, m.NID)))
			nskeysdel = append(nskeysdel, []byte(shardsNodeKey(m.VFI, m.SID)))
			msg.NodeID = m.NID
			nsb, err := proto.Marshal(msg)
			if err != nil {
				entry.WithError(err).Error("marshal failed")
				return err
			}
			nsbufs1 = append(nsbufs1, nsb)
		} else if m.SID == msg.NodeID && msg.NodeID == msg.NodeID2 {
			nskeys1 = append(nskeys1, []byte(shardsNodeKey(m.VFI, m.NID)))
			nskeysdel = append(nskeysdel, []byte(shardsNodeKey(m.VFI, m.SID)))
			msg.NodeID = m.NID
			msg.NodeID2 = m.NID
			nsb, err := proto.Marshal(msg)
			if err != nil {
				entry.WithError(err).Error("marshal failed")
				return err
			}
			nsbufs1 = append(nsbufs1, nsb)
		} else if m.SID == msg.NodeID && msg.NodeID2 == m.NID {
			nskeys1 = append(nskeys1, []byte(shardsNodeKey(m.VFI, m.NID)))
			nskeysdel = append(nskeysdel, []byte(shardsNodeKey(m.VFI, m.SID)))
			msg.NodeID = m.NID
			nsb, err := proto.Marshal(msg)
			if err != nil {
				entry.WithError(err).Error("marshal failed")
				return err
			}
			nsbufs1 = append(nsbufs1, nsb)
		} else if m.SID == msg.NodeID2 && msg.NodeID == m.NID {
			nskeys1 = append(nskeys1, []byte(shardsNodeKey(m.VFI, m.NID)))
			nskeysdel = append(nskeysdel, []byte(shardsNodeKey(m.VFI, m.SID)))
			msg.NodeID2 = m.NID
			nsb, err := proto.Marshal(msg)
			if err != nil {
				entry.WithError(err).Error("marshal failed")
				return err
			}
			nsbufs1 = append(nsbufs1, nsb)
		} else if m.SID == msg.NodeID && msg.NodeID2 != 0 {
			nskeys1 = append(nskeys1, []byte(shardsNodeKey(m.VFI, m.NID)))
			nskeysdel = append(nskeysdel, []byte(shardsNodeKey(m.VFI, m.SID)))
			msg.NodeID = m.NID
			nsb, err := proto.Marshal(msg)
			if err != nil {
				entry.WithError(err).Error("marshal failed")
				return err
			}
			nsbufs1 = append(nsbufs1, nsb)
			nskeys2 = append(nskeys2, []byte(shardsNodeKey(m.VFI, msg.NodeID2)))
			nsbufs2 = append(nsbufs2, nsb)
		} else if m.SID == msg.NodeID2 && msg.NodeID != 0 {
			nskeys1 = append(nskeys1, []byte(shardsNodeKey(m.VFI, m.NID)))
			nskeysdel = append(nskeysdel, []byte(shardsNodeKey(m.VFI, m.SID)))
			msg.NodeID2 = m.NID
			nsb, err := proto.Marshal(msg)
			if err != nil {
				entry.WithError(err).Error("marshal failed")
				return err
			}
			nsbufs1 = append(nsbufs1, nsb)
			nskeys2 = append(nskeys2, []byte(shardsNodeKey(m.VFI, msg.NodeID)))
			nsbufs2 = append(nsbufs2, nsb)
		} else {
			continue
		}

		// if m.SID == msg.NodeID {
		// 	nskeys1 = append(nskeys1, []byte(shardsNodeKey(m.VFI, m.NID)))
		// 	nskeysdel = append(nskeysdel, []byte(shardsNodeKey(m.VFI, m.SID)))
		// 	msg.NodeID = m.NID
		// 	nsb, err := proto.Marshal(msg)
		// 	if err != nil {
		// 		entry.WithError(err).Error("marshal failed")
		// 		return err
		// 	}
		// 	nsbufs1 = append(nsbufs1, nsb)
		// 	if msg.NodeID2 != 0 {
		// 		nskeys2 = append(nskeys2, []byte(shardsNodeKey(m.VFI, msg.NodeID2)))
		// 		nsbufs2 = append(nsbufs1, nsb)
		// 	}
		// } else if m.SID == msg.NodeID2 {
		// 	nskeys1 = append(nskeys1, []byte(shardsNodeKey(m.VFI, m.NID)))
		// 	nskeysdel = append(nskeysdel, []byte(shardsNodeKey(m.VFI, m.SID)))
		// 	msg.NodeID2 = m.NID
		// 	nsb, err := proto.Marshal(msg)
		// 	if err != nil {
		// 		entry.WithError(err).Error("marshal failed")
		// 		return err
		// 	}
		// 	nsbufs1 = append(nsbufs1, nsb)
		// 	if msg.NodeID != 0 {
		// 		nskeys2 = append(nskeys2, []byte(shardsNodeKey(m.VFI, msg.NodeID)))
		// 		nsbufs2 = append(nsbufs1, nsb)
		// 	}
		// }
		bkeysb = append(bkeysb, []byte(blocksKey(msg.BlockID)))
		bshards = append(bshards, msg)
	}
	bkeysb2 := make([][]byte, 0)
	bbufs := make([][]byte, 0)
	bufs2, err := tikvDao.cli.BatchGet(ctx, bkeysb)
	if err != nil {
		entry.WithError(err).Error("batch get failed")
		return err
	}
	for i := 0; i < len(bkeysb); i++ {
		b := bufs2[i]
		if b == nil {
			continue
		}
		bkeysb2 = append(bkeysb2, bkeysb[i])
		srd := bshards[i]
		msg := new(pb.BlockMsg)
		err := proto.Unmarshal(b, msg)
		if err != nil {
			entry.WithError(err).Error("unmarshal failed")
			return err
		}
		for _, s := range msg.Shards {
			if s.Id == srd.Id {
				s.NodeID = srd.NodeID
				s.NodeID2 = srd.NodeID2
			}
		}
		nsb2, err := proto.Marshal(msg)
		if err != nil {
			entry.WithError(err).Error("marshal failed")
			return err
		}
		bbufs = append(bbufs, nsb2)
	}
	err = tikvDao.cli.BatchPut(ctx, bkeysb2, bbufs)
	if err != nil {
		entry.WithError(err).Error("batch put blocks error: 1")
		return err
	}
	err = tikvDao.cli.BatchPut(ctx, nskeys1, nsbufs1)
	if err != nil {
		entry.WithError(err).Error("batch put node shards error: 2")
		return err
	}
	err = tikvDao.cli.BatchPut(ctx, nskeys2, nsbufs2)
	if err != nil {
		entry.WithError(err).Error("batch put node shards error: 3")
		return err
	}
	err = tikvDao.cli.BatchDelete(ctx, nskeysdel)
	if err != nil {
		entry.WithError(err).Error("batch delete node shards error: 4")
		return err
	}
	return nil

	// keys1 := make([][]byte, 0)
	// keys1new := make([][]byte, 0)
	// keys2old := make([][]byte, 0)
	// keys2new1 := make([][]byte, 0)
	// keys2new2 := make([][]byte, 0)
	// vals1 := make([][]byte, 0)
	// vals2 := make([][]byte, 0)
	// for _, m := range metas {
	// 	keys1 = append(keys1, []byte(shardsKey(m.VFI)))
	// }
	// bufs, err := tikvDao.cli.BatchGet(ctx, keys1)
	// if err != nil {
	// 	entry.WithError(err).Error("batch get failed")
	// 	return err
	// }
	// for i := 0; i < len(bufs); i++ {
	// 	b := bufs[i]
	// 	if b == nil {
	// 		continue
	// 	}
	// 	msg := new(pb.ShardMsg)
	// 	err := proto.Unmarshal(b, msg)
	// 	if err != nil {
	// 		entry.WithError(err).Error("unmarshal failed")
	// 		return err
	// 	}
	// 	if msg.NodeID != metas[i].SID && msg.NodeID2 != metas[i].SID {
	// 		continue
	// 	}

	// 	if msg.NodeID == metas[i].SID {
	// 		msg.NodeID = metas[i].NID
	// 		b, err = proto.Marshal(msg)
	// 		if err != nil {
	// 			entry.WithError(err).Error("marshal failed")
	// 			return err
	// 		}
	// 		keys2new1 = append(keys2new1, []byte(shardsNodeKey(metas[i].VFI, msg.NodeID)))
	// 		vals1 = append(vals1, b)
	// 		if msg.NodeID2 > 0 {
	// 			keys2new2 = append(keys2new2, []byte(shardsNodeKey(metas[i].VFI, msg.NodeID2)))
	// 			vals2 = append(vals2, b)
	// 		}
	// 	} else if msg.NodeID2 == metas[i].SID {
	// 		msg.NodeID2 = metas[i].NID
	// 		b, err = proto.Marshal(msg)
	// 		if err != nil {
	// 			entry.WithError(err).Error("marshal failed")
	// 			return err
	// 		}
	// 		keys2new1 = append(keys2new1, []byte(shardsNodeKey(metas[i].VFI, msg.NodeID2)))
	// 		vals1 = append(vals1, b)
	// 		if msg.NodeID > 0 {
	// 			keys2new2 = append(keys2new2, []byte(shardsNodeKey(metas[i].VFI, msg.NodeID)))
	// 			vals2 = append(vals2, b)
	// 		}
	// 	} else {
	// 		continue
	// 	}
	// 	keys1new = append(keys1new, []byte(shardsKey(metas[i].VFI)))
	// 	keys2old = append(keys2old, []byte(shardsNodeKey(metas[i].VFI, metas[i].SID)))
	// }
	// err = tikvDao.cli.BatchDelete(ctx, keys2old)
	// if err != nil {
	// 	entry.WithError(err).Error("batch delete error")
	// 	return err
	// }
	// err = tikvDao.cli.BatchPut(ctx, keys2new1, vals1)
	// if err != nil {
	// 	entry.WithError(err).Error("batch put error: 2")
	// 	return err
	// }
	// err = tikvDao.cli.BatchPut(ctx, keys2new2, vals2)
	// if err != nil {
	// 	entry.WithError(err).Error("batch put error: 3")
	// 	return err
	// }
	// err = tikvDao.cli.BatchPut(ctx, keys1new, vals1)
	// if err != nil {
	// 	entry.WithError(err).Error("batch put error: 1")
	// 	return err
	// }
	// return nil
}

//DeleteBlocks delete blocks
func (tikvDao *TikvDao) DeletBlocks(ctx context.Context, blocks []*BlockDel) error {
	entry := log.WithFields(log.Fields{Function: "DeletBlocks"})
	if len(blocks) == 0 {
		return nil
	}
	keys := make([][]byte, 0)
	keysdel := make([][]byte, 0)
	for _, b := range blocks {
		keys = append(keys, []byte(blocksKey(b.VBI)))
	}
	bufs, err := tikvDao.cli.BatchGet(ctx, keys)
	if err != nil {
		entry.WithError(err).Error("batch get failed")
		return err
	}
	for i := 0; i < len(bufs); i++ {
		b := bufs[i]
		if b == nil {
			continue
		}
		block := new(Block)
		err := block.FillBytes(b)
		if err != nil {
			entry.WithError(err).Error("decode failed")
			return err
		}
		for _, shard := range block.Shards {
			keysdel = append(keysdel, []byte(shardsNodeKey(shard.ID, shard.NodeID)))
			if shard.NodeID2 != 0 && shard.NodeID != shard.NodeID2 {
				keysdel = append(keysdel, []byte(shardsNodeKey(shard.ID, shard.NodeID2)))
			}
		}
	}
	err = tikvDao.cli.BatchDelete(ctx, keysdel)
	if err != nil {
		entry.WithError(err).Error("batch delete node shards error")
		return err
	}
	err = tikvDao.cli.BatchDelete(ctx, keys)
	if err != nil {
		entry.WithError(err).Error("batch delete blocks error")
		return err
	}
	return nil
}

func blocksKey(id int64) string {
	return fmt.Sprintf("%s_%d", PFX_BLOCKS, id)
}

func shardsKey(id int64) string {
	return fmt.Sprintf("%s_%d", PFX_SHARDS, id)
}

func checkpointKey(id int32) string {
	return fmt.Sprintf("%s_%d", PFX_CHECKPOINT, id)
}

func shardsNodeKey(id int64, nodeId int32) string {
	return fmt.Sprintf("%s_%d_%d", PFX_SHARDNODES, nodeId, id)
}
