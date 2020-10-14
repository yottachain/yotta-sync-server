package ytsync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	log "github.com/sirupsen/logrus"
	"github.com/tylerb/graceful"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//Server server struct
type Server struct {
	server *echo.Echo
	dao    *ServerDao
}

//ServerDao data access object of server
type ServerDao struct {
	dbCli    *mongo.Client
	dbName   string
	SNID     int
	SkipTime int
}

//NewServer create new server instance
func NewServer(ctx context.Context, mongoDBURL, dbName string, snID, skipTime int) (*Server, error) {
	entry := log.WithFields(log.Fields{Function: "NewServer"})
	dbClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoDBURL))
	if err != nil {
		entry.WithError(err).Errorf("creating mongo DB client failed: %s", mongoDBURL)
		return nil, err
	}
	entry.Infof("mongoDB connected: %s", mongoDBURL)
	server := echo.New()
	return &Server{server: server, dao: &ServerDao{dbCli: dbClient, dbName: dbName, SNID: snID, SkipTime: skipTime}}, nil
}

//StartServer HTTP server
func (server *Server) StartServer(bindAddr string) error {
	entry := log.WithFields(log.Fields{Function: "StartServer"})
	server.server.Use(middleware.Logger())
	server.server.Use(middleware.Recover())
	server.server.Use(middleware.GzipWithConfig(middleware.GzipConfig{
		Level: 5,
	}))
	server.server.GET("sync/getSyncData", server.GetSyncData)
	server.server.GET("sync/getShardRebuildMetas", server.GetRebuildData)
	server.server.GET("sync/GetStoredShards", server.GetStoredShards)
	server.server.Server.Addr = bindAddr
	err := graceful.ListenAndServe(server.server.Server, 5*time.Second)
	if err != nil {
		entry.WithError(err).Error("start sync server failed")
	}
	return err
}

//SyncQuery struct
type SyncQuery struct {
	From int64 `json:"from" form:"from" query:"from"`
	Size int64 `json:"size" form:"size" query:"size"`
	Skip int64 `json:"skip" form:"skip" query:"skip"`
}

//RebuildQuery struct
type RebuildQuery struct {
	Start int64 `json:"start" form:"start" query:"start"`
	Count int64 `json:"count" form:"count" query:"count"`
}

//StoredShardsQuery struct
type StoredShardsQuery struct {
	From int32 `json:"from" form:"from" query:"from"`
	To   int32 `json:"to" form:"to" query:"to"`
}

//GetSyncData fetch sync data
func (server *Server) GetSyncData(c echo.Context) error {
	entry := log.WithFields(log.Fields{Function: "GetSyncData"})
	q := new(SyncQuery)
	if err := c.Bind(q); err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	resp, err := server.dao.GetSyncData(context.Background(), q.From, q.Size, q.Skip)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	b, err := json.Marshal(resp)
	if err != nil {
		entry.WithError(err).Error("marshaling data to json")
		return c.String(http.StatusInternalServerError, err.Error())
	}
	return c.JSONBlob(http.StatusOK, b)
}

//GetRebuildData fetch rebuilt data
func (server *Server) GetRebuildData(c echo.Context) error {
	entry := log.WithFields(log.Fields{Function: "GetRebuildData"})
	q := new(RebuildQuery)
	if err := c.Bind(q); err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	resp, err := server.dao.GetRebuildData(context.Background(), q.Start, q.Count)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	if resp == nil {
		resp = make([]*ShardRebuildMeta, 0)
	}
	b, err := json.Marshal(resp)
	if err != nil {
		entry.WithError(err).Error("marshaling data to json")
		return c.String(http.StatusInternalServerError, err.Error())
	}
	return c.JSONBlob(http.StatusOK, b)
}

//GetStoredShards fetch stored shards
func (server *Server) GetStoredShards(c echo.Context) error {
	entry := log.WithFields(log.Fields{Function: "GetStoredShards"})
	q := new(StoredShardsQuery)
	if err := c.Bind(q); err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	if int64(q.To) > time.Now().Unix()-int64(server.dao.SkipTime) {
		return c.String(http.StatusInternalServerError, "time range is invalid")
	}
	padding := []byte{0x00, 0x00, 0x00, 0x00}
	fromByte32 := Int32ToBytes(q.From)
	from64 := BytesToInt64(append(fromByte32, padding...))
	toByte32 := Int32ToBytes(q.To)
	to64 := BytesToInt64(append(toByte32, padding...))
	resp, err := server.dao.GetStoredShards(context.Background(), from64, to64)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	if resp == nil {
		resp = make([]*Shard, 0)
	}
	b, err := json.Marshal(resp)
	if err != nil {
		entry.WithError(err).Error("marshaling data to json")
		return c.String(http.StatusInternalServerError, err.Error())
	}
	return c.JSONBlob(http.StatusOK, b)
}

//GetSyncData get range data
func (dao *ServerDao) GetSyncData(ctx context.Context, from, size, skip int64) (*DataResp, error) {
	entry := log.WithFields(log.Fields{Function: "GetSyncData"})
	if size == 0 || skip == 0 {
		err := fmt.Errorf("invalid parameters: from -> %d, size -> %d, skip -> %d", from, size, skip)
		entry.WithError(err).Error("invalid parameters")
		return nil, err
	}
	resp := &DataResp{SNID: dao.SNID, From: from, Blocks: make([]*Block, 0), Shards: make([]*Shard, 0), Rebuilds: make([]*ShardRebuildMeta, 0)}
	blockTab := dao.dbCli.Database(dao.dbName).Collection(BlocksTab)
	shardsTab := dao.dbCli.Database(dao.dbName).Collection(ShardsTab)
	rebuildTab := dao.dbCli.Database(dao.dbName).Collection(ShardsRebuildTab)
	//fetch blocks
	size++
	opts := new(options.FindOptions)
	opts.Sort = bson.M{"_id": 1}
	opts.Limit = &size
	skipByte32 := Int32ToBytes(int32(time.Now().Unix() - skip))
	padding := []byte{0x00, 0x00, 0x00, 0x00}
	skipTime64 := BytesToInt64(append(skipByte32, padding...))
	bCur, err := blockTab.Find(ctx, bson.M{"_id": bson.M{"$gte": from, "$lt": skipTime64}}, opts)
	if err != nil {
		entry.WithError(err).Errorf("traversal blocks: from -> %d, size -> %d, skip -> %d", from, size, skip)
		return nil, err
	}
	defer bCur.Close(ctx)
	blocks := make([]*Block, 0)
	for bCur.Next(ctx) {
		block := new(Block)
		err := bCur.Decode(block)
		if err != nil {
			entry.WithError(err).Errorf("decoding block failed: from -> %d, size -> %d, skip -> %d", from, size, skip)
			return nil, err
		}
		blocks = append(blocks, block)
	}
	if len(blocks) == 0 {
		resp.More = false
		resp.Next = resp.From
		resp.Size = 0
		return resp, nil
	}
	if int64(len(blocks)) == size {
		resp.Blocks = blocks[0 : size-1]
		resp.More = true
		resp.Size = size - 1
	} else {
		resp.Blocks = blocks
		resp.More = false
		resp.Size = int64(len(resp.Blocks))
	}
	resp.Next = resp.Blocks[len(resp.Blocks)-1].ID + int64(resp.Blocks[len(resp.Blocks)-1].VNF)
	var innerErr *error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		//fetch shards
		defer wg.Done()
		opts = new(options.FindOptions)
		opts.Sort = bson.M{"_id": 1}
		sCur, err := shardsTab.Find(ctx, bson.M{"_id": bson.M{"$gte": resp.Blocks[0].ID, "$lt": resp.Next}}, opts)
		if err != nil {
			entry.WithError(err).Errorf("traversal shards: from -> %d, end -> %d", resp.From, resp.Next)
			innerErr = &err
			return
		}
		defer sCur.Close(ctx)
		shards := make([]*Shard, 0)
		for sCur.Next(ctx) {
			shard := new(Shard)
			err := sCur.Decode(shard)
			if err != nil {
				entry.WithError(err).Errorf("decoding shard failed: from -> %d, end -> %d", resp.From, resp.Next)
				innerErr = &err
				return
			}
			shards = append(shards, shard)
		}
		shards2 := make([]*Shard, 0)
		j := 0
		for _, b := range resp.Blocks {
			k := 0
			for i := b.ID; i < b.ID+int64(b.VNF); i++ {
				if j >= len(shards) {
					err := fmt.Errorf("index of shards not match, block: %d", b.ID)
					entry.WithError(err).Error("validate shards")
					innerErr = &err
					return
				}
				if i == shards[j].ID {
					shards[j].BlockID = b.ID
					shards2 = append(shards2, shards[j])
					k++
				} else {
					i--
				}
				j++
			}
			if int32(k) != b.VNF {
				err := fmt.Errorf("lost shards of block: %d", b.ID)
				entry.WithError(err).Error("validate shards")
				innerErr = &err
				return
			}
		}
		resp.Shards = shards2
	}()
	go func() {
		//fetch rebuild meta
		defer wg.Done()
		opts = new(options.FindOptions)
		opts.Sort = bson.M{"_id": 1}
		rCur, err := rebuildTab.Find(ctx, bson.M{"_id": bson.M{"$gte": resp.From, "$lt": resp.Next}}, opts)
		if err != nil {
			entry.WithError(err).Errorf("traversal rebuilt shards: from -> %d, end -> %d", resp.From, resp.Next)
			innerErr = &err
			return
		}
		defer rCur.Close(ctx)
		rebuilds := make([]*ShardRebuildMeta, 0)
		for rCur.Next(ctx) {
			rshard := new(ShardRebuildMeta)
			err := rCur.Decode(rshard)
			if err != nil {
				entry.WithError(err).Errorf("decoding rebuilt shard failed: from -> %d, end -> %d", resp.From, resp.Next)
				innerErr = &err
				return
			}
			rebuilds = append(rebuilds, rshard)
		}
		resp.Rebuilds = rebuilds
	}()
	wg.Wait()
	if innerErr != nil {
		return nil, *innerErr
	}
	return resp, nil
}

//GetRebuildData fetch rebuild metadata
func (dao *ServerDao) GetRebuildData(ctx context.Context, start, count int64) ([]*ShardRebuildMeta, error) {
	entry := log.WithFields(log.Fields{Function: "GetRebuildData"})
	if count == 0 {
		err := fmt.Errorf("invalid parameters: start -> %d, count -> %d", start, count)
		entry.WithError(err).Error("invalid parameters")
		return nil, err
	}
	rebuildTab := dao.dbCli.Database(dao.dbName).Collection(ShardsRebuildTab)
	opts := new(options.FindOptions)
	opts.Sort = bson.M{"_id": 1}
	opts.Limit = &count
	rCur, err := rebuildTab.Find(ctx, bson.M{"_id": bson.M{"$gte": start}}, opts)
	if err != nil {
		entry.WithError(err).Errorf("traversal rebuild shards: start -> %d, count -> %d", start, count)
		return nil, err
	}
	defer rCur.Close(ctx)
	rebuilds := make([]*ShardRebuildMeta, 0)
	for rCur.Next(ctx) {
		rshard := new(ShardRebuildMeta)
		err := rCur.Decode(rshard)
		if err != nil {
			entry.WithError(err).Errorf("decoding rebuild metadata failed: start -> %d, count -> %d", start, count)
			return nil, err
		}
		rebuilds = append(rebuilds, rshard)
	}
	return rebuilds, nil
}

//GetStoredShards get stored shards in period
func (dao *ServerDao) GetStoredShards(ctx context.Context, from, to int64) ([]*Shard, error) {
	entry := log.WithFields(log.Fields{Function: "GetStoredShards"})
	if from < 0 || to < 0 || from >= to {
		err := fmt.Errorf("invalid parameters: from -> %d, to -> %d", from, to)
		entry.WithError(err).Error("invalid parameters")
		return nil, err
	}
	shardsTab := dao.dbCli.Database(dao.dbName).Collection(ShardsTab)
	rebuildTab := dao.dbCli.Database(dao.dbName).Collection(ShardsRebuildTab)
	shards1 := make([]*Shard, 0)
	shards2 := make([]*Shard, 0)
	var innerErr *error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		//fetch shards
		defer wg.Done()
		opts := new(options.FindOptions)
		opts.Sort = bson.M{"_id": 1}
		sCur, err := shardsTab.Find(ctx, bson.M{"_id": bson.M{"$gte": from, "$lt": to}}, opts)
		if err != nil {
			entry.WithError(err).Errorf("traversal shards: from -> %d, to -> %d", from, to)
			innerErr = &err
			return
		}
		defer sCur.Close(ctx)
		for sCur.Next(ctx) {
			shard := new(Shard)
			err := sCur.Decode(shard)
			if err != nil {
				entry.WithError(err).Errorf("decoding shard failed: from -> %d, to -> %d", from, to)
				innerErr = &err
				return
			}
			shards1 = append(shards1, shard)
		}
	}()
	go func() {
		//fetch rebuilt shards
		defer wg.Done()
		opts := new(options.FindOptions)
		opts.Sort = bson.M{"_id": 1}
		rCur, err := rebuildTab.Find(ctx, bson.M{"_id": bson.M{"$gte": from, "$lt": to}}, opts)
		if err != nil {
			entry.WithError(err).Errorf("traversal rebuild shards: from -> %d, to -> %d", from, to)
			innerErr = &err
			return
		}
		defer rCur.Close(ctx)
		for rCur.Next(ctx) {
			rshard := new(ShardRebuildMeta)
			err := rCur.Decode(rshard)
			if err != nil {
				entry.WithError(err).Errorf("decoding rebuild metadata failed: from -> %d, to -> %d", from, to)
				innerErr = &err
				return
			}
			shard := new(Shard)
			err = shardsTab.FindOne(ctx, bson.M{"_id": rshard.VFI}).Decode(shard)
			if err != nil {
				entry.WithError(err).Errorf("decoding shard %d failed: from -> %d, to -> %d", rshard.VFI, from, to)
				innerErr = &err
				return
			}
			shards2 = append(shards2, shard)
		}
	}()
	wg.Wait()
	if innerErr != nil {
		return nil, *innerErr
	}
	return append(shards1, shards2...), nil
}
