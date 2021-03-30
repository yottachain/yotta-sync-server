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
	keys1 := make([][]byte, 0)
	keys2 := make([][]byte, 0)
	vals := make([][]byte, 0)
	for _, s := range shards {
		buf, err := s.ConvertBytes()
		if err != nil {
			entry.WithError(err).Errorf("convert error: %v", s)
			return err
		}
		keys1 = append(keys1, []byte(shardsKey(s.ID)))
		keys2 = append(keys2, []byte(shardsNodeKey(s.ID, s.NodeID)))
		vals = append(vals, buf)
	}
	err := tikvDao.cli.BatchPut(ctx, keys1, vals)
	if err != nil {
		entry.WithError(err).Error("batch put error: 1")
		return err
	}
	err = tikvDao.cli.BatchPut(ctx, keys2, vals)
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
	keys1new := make([][]byte, 0)
	keys2old := make([][]byte, 0)
	keys2new := make([][]byte, 0)
	vals := make([][]byte, 0)
	for _, m := range metas {
		keys1 = append(keys1, []byte(shardsKey(m.VFI)))

	}
	bufs, err := tikvDao.cli.BatchGet(ctx, keys1)
	if err != nil {
		entry.WithError(err).Error("batch get failed")
		return err
	}
	for i := 0; i < len(bufs); i++ {
		b := bufs[i]
		if b == nil {
			continue
		}
		keys1new = append(keys1new, []byte(shardsKey(metas[i].VFI)))
		keys2old = append(keys2old, []byte(shardsNodeKey(metas[i].VFI, metas[i].SID)))
		keys2new = append(keys2new, []byte(shardsNodeKey(metas[i].VFI, metas[i].NID)))
		msg := new(pb.ShardMsg)
		err := proto.Unmarshal(b, msg)
		if err != nil {
			entry.WithError(err).Error("unmarshal failed")
			return err
		}
		msg.NodeID = metas[i].NID
		buf, err := proto.Marshal(msg)
		if err != nil {
			entry.WithError(err).Error("marshal failed")
			return err
		}
		vals = append(vals, buf)
	}
	err = tikvDao.cli.BatchPut(ctx, keys1new, vals)
	if err != nil {
		entry.WithError(err).Error("batch put error: 1")
		return err
	}
	err = tikvDao.cli.BatchPut(ctx, keys2new, vals)
	if err != nil {
		entry.WithError(err).Error("batch put error: 2")
		return err
	}
	err = tikvDao.cli.BatchDelete(ctx, keys2old)
	if err != nil {
		entry.WithError(err).Error("batch delete error")
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
