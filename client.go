package ytsync

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	log "github.com/sirupsen/logrus"
	"github.com/tylerb/graceful"
	ytab "github.com/yottachain/yotta-arraybase"
)

var checkPoints map[int32]int64

//Client client struct
type Client struct {
	server    *echo.Echo
	httpCli   *http.Client
	tikvCli   *TikvDao
	arraybase *ytab.ArrayBase
	SyncURLs  []string
	StartTime int32
	BatchSize int
	WaitTime  int
	SkipTime  int
	lock      sync.RWMutex
}

//NewClient create a new client instance
func NewClient(ctx context.Context, baseDir string, rowsPerFile uint64, readChLen, writeChLen int, pdURLs []string, syncURLs []string, startTime int32, batchSize, waitTime, skipTime int) (*Client, error) {
	entry := log.WithFields(log.Fields{Function: "NewClient"})
	tikvCli, err := NewTikvDao(ctx, pdURLs)
	if err != nil {
		entry.WithError(err).Errorf("creating tikv client failed: %v", pdURLs)
		return nil, err
	}
	server := echo.New()
	arraybase, err := ytab.InitArrayBase(ctx, baseDir, rowsPerFile, readChLen, writeChLen)
	if err != nil {
		entry.WithError(err).Errorf("creating arraybase failed: basedir->%s, rowsperfile->%d, readchlen->%d, writechlen->%d", baseDir, rowsPerFile, readChLen, writeChLen)
		return nil, err
	}
	return &Client{server: server, httpCli: &http.Client{}, tikvCli: tikvCli, arraybase: arraybase, SyncURLs: syncURLs, StartTime: startTime, BatchSize: batchSize, WaitTime: waitTime, SkipTime: skipTime}, nil
}

//StartClient start client service
func (cli *Client) StartClient(ctx context.Context, bindAddr string) error {
	checkPoints = make(map[int32]int64)
	urls := cli.SyncURLs
	snCount := len(urls)
	for i := 0; i < snCount; i++ {
		snID := int32(i)
		checkPoints[snID] = 0
	}
	cli.server.Use(middleware.Logger())
	cli.server.Use(middleware.Recover())
	cli.server.Use(middleware.GzipWithConfig(middleware.GzipConfig{
		Level: 5,
	}))
	cli.server.GET("getCheckPoints", cli.GetCheckPoints)
	cli.server.GET("getBlocks", cli.GetBlocks)
	cli.server.Server.Addr = bindAddr
	go func() {
		err := graceful.ListenAndServe(cli.server.Server, 5*time.Second)
		if err != nil {
			log.WithError(err).Errorf("start http server failed: %s", bindAddr)
		}
	}()

	for i := 0; i < snCount; i++ {
		snID := int32(i)
		go func() {
			entry := log.WithFields(log.Fields{Function: "StartClient", SNID: snID})
			entry.Info("starting synchronization process")
			for {
				checkPoint, err := cli.tikvCli.FindCheckPoint(ctx, snID)
				if err != nil {
					if err == NoValError {
						entry.Infof("cannot find checkpoint of SN%d, read start time from config file...", snID)
						startBytes32 := Int32ToBytes(cli.StartTime)
						padding := []byte{0x00, 0x00, 0x00, 0x00}
						startTime64 := BytesToInt64(append(startBytes32, padding...))
						checkPoint = &CheckPoint{ID: snID, Start: startTime64, Timestamp: time.Now().Unix()}
						err := cli.tikvCli.InsertCheckPoint(ctx, checkPoint)
						if err != nil {
							entry.WithError(err).Errorf("insert checkpoint: %d", snID)
							time.Sleep(time.Duration(cli.WaitTime) * time.Second)
							continue
						}
						entry.Infof("insert checkpoint of SN%d: %d", snID, checkPoint.Start)
						cli.lock.Lock()
						checkPoints[snID] = int64(cli.StartTime) * 1000000000
						cli.lock.Unlock()
					} else {
						entry.WithError(err).Errorf("find checkpoint of SN%d", snID)
						time.Sleep(time.Duration(cli.WaitTime) * time.Second)
						continue
					}
				}
				start := time.Now().UnixMilli()
				begin := start
				entry.Debugf("ready for fetching sync data")
				resp, err := GetSyncData(cli.httpCli, cli.SyncURLs[snID], checkPoint.Start, cli.BatchSize, cli.SkipTime)
				if err != nil {
					entry.WithError(err).Errorf("Get sync data of SN%d", snID)
					time.Sleep(time.Duration(cli.WaitTime) * time.Second)
					continue
				}
				if snID != int32(resp.SNID) {
					entry.Fatalf("received SN ID not match current process: %d", resp.SNID)
				}
				entry.Debugf("received response of SN%d cost %d ms: %d blocks, %d rebuilds, %s", resp.SNID, time.Now().UnixMilli()-start, len(resp.Blocks), len(resp.Rebuilds), func() string {
					if resp.More {
						return "have more data"
					}
					return "no more data"
				}())
				//block转换为arraybase格式
				start = time.Now().UnixMilli()
				blocksAB := make([]*ytab.Block, 0)
				for _, b := range resp.Blocks {
					blocksAB = append(blocksAB, b.ConvertToAB())
				}
				entry.Debugf("convert format of blocks in SN%d to arraybase format, cost %d ms", resp.SNID, time.Now().UnixMilli()-start)
				//重建数据转换为arraybase格式
				start = time.Now().UnixMilli()
				rebuildsAB, err := cli.tikvCli.FindShardMetas(ctx, resp.Rebuilds)
				if err != nil {
					entry.WithError(err).Errorf("Convert rebuild metas of SN%d", snID)
					time.Sleep(time.Duration(cli.WaitTime) * time.Second)
					continue
				}
				entry.Debugf("convert format of shardmetas in SN%d to arraybase format, cost %d ms", resp.SNID, time.Now().UnixMilli()-start)
				//删除数据转换为arraybase格式
				start = time.Now().UnixMilli()
				deletesAB, err := cli.tikvCli.FindDeleteMetas(ctx, resp.BlockDel)
				if err != nil {
					entry.WithError(err).Errorf("Convert delete metas of SN%d", snID)
					time.Sleep(time.Duration(cli.WaitTime) * time.Second)
					continue
				}
				entry.Debugf("convert format of deletes in SN%d to arraybase format, cost %d ms", resp.SNID, time.Now().UnixMilli()-start)
				// //预处理tikv中要删除的block
				// deletedIDs := make([]uint64, 0)
				// for _, bdel := range deletesAB {
				// 	deletedIDs = append(deletedIDs, bdel)
				// }
				start = time.Now().UnixMilli()
				dblocks, err := cli.arraybase.Read(deletesAB)
				if err != nil {
					entry.WithError(err).Errorf("read blocks of SN%d before deleted", snID)
					time.Sleep(time.Duration(cli.WaitTime) * time.Second)
					continue
				}
				entry.Debugf("read deleted blocks in arraybase of SN%d cost %d ms", resp.SNID, time.Now().UnixMilli()-start)
				start = time.Now().UnixMilli()
				deletedKeys := make([][]byte, 0)
				for _, dblk := range dblocks {
					if dblk != nil {
						for k, v := range dblk.Shards {
							//deletedKeys = append(deletedKeys, []byte(shardmetasKey(int64(dblk.ID)+int64(k))))
							if v.NodeID != 0 {
								deletedKeys = append(deletedKeys, []byte(shardsNodeKey(int64(dblk.ID)+int64(k), int32(v.NodeID))))
							}
							if v.NodeID2 != 0 {
								deletedKeys = append(deletedKeys, []byte(shardsNodeKey(int64(dblk.ID)+int64(k), int32(v.NodeID2))))
							}
						}
					}
				}
				//先把重建中的源分片删除
				for _, rebuild := range resp.Rebuilds {
					deletedKeys = append(deletedKeys, []byte(shardsNodeKey(rebuild.VFI, rebuild.SID)))
				}
				err = cli.tikvCli.BatchDelete(ctx, deletedKeys, 10000)
				if err != nil {
					entry.WithError(err).Errorf("delete node shards of SN%d", snID)
					time.Sleep(time.Duration(cli.WaitTime) * time.Second)
					continue
				}
				entry.Debugf("delete blocks in tikv of SN%d cost %d ms", resp.SNID, time.Now().UnixMilli()-start)
				//修改写入arraybase
				start = time.Now().UnixMilli()
				from, to, err := cli.arraybase.Write(blocksAB, rebuildsAB, deletesAB)
				if err != nil {
					entry.WithError(err).Errorf("write arraybase of SN%d", snID)
					time.Sleep(time.Duration(cli.WaitTime) * time.Second)
					continue
				}
				entry.Debugf("modify arraybase of SN%d cost %d ms", resp.SNID, time.Now().UnixMilli()-start)
				//shard写入tikv
				start = time.Now().UnixMilli()
				err = cli.tikvCli.InsertShardMetas(ctx, blocksAB, from, to)
				if err != nil {
					entry.WithError(err).Errorf("insert shard metas of SN%d", snID)
					time.Sleep(time.Duration(cli.WaitTime) * time.Second)
					continue
				}
				entry.Debugf("write shardmetas in tikv of SN%d cost %d ms", resp.SNID, time.Now().UnixMilli()-start)
				//写入重建后分片
				start = time.Now().UnixMilli()
				err = cli.tikvCli.InsertNodeShards(ctx, cli.arraybase, rebuildsAB)
				if err != nil {
					entry.WithError(err).Errorf("update rebuilt shards of SN%d", snID)
					time.Sleep(time.Duration(cli.WaitTime) * time.Second)
					continue
				}
				entry.Debugf("write nodeshards in tikv of SN%d cost %d ms", resp.SNID, time.Now().UnixMilli()-start)
				entry.Debugf("total operation of SN%d cost %d ms", resp.SNID, time.Now().UnixMilli()-begin)
				// rebuildIndexes := make([]uint64, 0)
				// for _, v := range rebuildsAB {
				// 	rebuildIndexes = append(rebuildIndexes, v.BIndex)
				// }
				// rebuildBlocks, err := cli.arraybase.Read(rebuildIndexes)
				// if err != nil {
				// 	entry.WithError(err).Errorf("read rebuilt blocks of SN%d", snID)
				// 	time.Sleep(time.Duration(cli.WaitTime) * time.Second)
				// 	continue
				// }
				// keys := make([][]byte, 0)
				// vals := make([][]byte, 0)
				// for _, v := range rebuildsAB {
				// 	r := rebuildBlocks[v.BIndex]
				// 	if r.ID == 0 {
				// 		continue
				// 	}
				// 	keys = append(keys, []byte(shardsNodeKey(int64(r.ID)+int64(v.Offset), int32(r.Shards[int(v.Offset)].NodeID))))
				// 	msg := new(pb.ShardMsg)
				// 	msg.Id = int64(r.ID) + int64(v.Offset)
				// 	msg.Vhf = r.Shards[int(v.Offset)].VHF
				// 	msg.Bindex = v.BIndex
				// 	msg.NodeID = int32(r.Shards[int(v.Offset)].NodeID)
				// 	msg.NodeID2 = int32(r.Shards[int(v.Offset)].NodeID2)
				// 	msg.Timestamp = time.Now().Unix()
				// 	buf, err := proto.Marshal(msg)
				// 	if err != nil {
				// 		entry.WithError(err).Errorf("convert ShardMsg error: %v", msg)
				// 		break
				// 	}
				// 	vals = append(vals, buf)
				// 	if r.Shards[int(v.Offset)].NodeID2 != 0 {
				// 		keys = append(keys, []byte(shardsNodeKey(int64(r.ID)+int64(v.Offset), int32(r.Shards[int(v.Offset)].NodeID2))))
				// 		vals = append(vals, buf)
				// 	}
				// }
				// err = cli.tikvCli.cli.BatchPut(ctx, keys, vals)
				// if err != nil {
				// 	entry.WithError(err).Errorf("update rebuilt shards of SN%d", snID)
				// 	time.Sleep(time.Duration(cli.WaitTime) * time.Second)
				// 	continue
				// }
				// //err = cli.tikvCli.BatchDelete(ctx, deletedKeys, 10000)
				// err = cli.tikvCli.cli.BatchDelete(ctx, deletedKeys)
				// if err != nil {
				// 	entry.WithError(err).Errorf("delete shard metas and node shards of SN%d", snID)
				// 	time.Sleep(time.Duration(cli.WaitTime) * time.Second)
				// 	continue
				// }

				// var innerErr *error
				// var wg sync.WaitGroup
				// wg.Add(4)
				// go func() {
				// 	defer wg.Done()
				// 	if len(resp.Blocks) > 0 {
				// 		// for _, b := range resp.Blocks {
				// 		// 	b.SnID = int32(resp.SNID)
				// 		// }
				// 		err := cli.tikvCli.InsertBlocks(ctx, resp.Blocks)
				// 		if err != nil {
				// 			entry.WithError(err).Errorf("insert blocks of SN%d", snID)
				// 			innerErr = &err
				// 		} else {
				// 			entry.Infof("%d blocks inserted", len(resp.Blocks))
				// 		}
				// 	}
				// }()
				// go func() {
				// 	//insert shards
				// 	defer wg.Done()
				// 	if len(resp.Blocks) > 0 {
				// 		shards := make([]*Shard, 0)
				// 		for _, b := range resp.Blocks {
				// 			shards = append(shards, b.Shards...)
				// 		}
				// 		err := cli.tikvCli.InsertShards(ctx, shards)
				// 		if err != nil {
				// 			entry.WithError(err).Errorf("insert shards of SN%d", snID)
				// 			innerErr = &err
				// 		} else {
				// 			entry.Infof("%d shards inserted", len(shards))
				// 		}
				// 	}
				// }()
				// go func() {
				// 	//update rebuilt shards
				// 	defer wg.Done()
				// 	if len(resp.Rebuilds) > 0 {
				// 		var count int64 = 10000
				// 		metas := make([][]*ShardRebuildMeta, 0)
				// 		max := int64(len(resp.Rebuilds))
				// 		if max < count {
				// 			metas = append(metas, resp.Rebuilds)
				// 		} else {
				// 			var quantity int64
				// 			if max%count == 0 {
				// 				quantity = max / count
				// 			} else {
				// 				quantity = (max / count) + 1
				// 			}
				// 			var start, end, i int64
				// 			for i = 1; i <= quantity; i++ {
				// 				end = i * count
				// 				if i != quantity {
				// 					metas = append(metas, resp.Rebuilds[start:end])
				// 				} else {
				// 					metas = append(metas, resp.Rebuilds[start:])
				// 				}
				// 				start = i * count
				// 			}
				// 		}
				// 		for index, m := range metas {
				// 			err := cli.tikvCli.UpdateShards(ctx, m)
				// 			if err != nil {
				// 				entry.WithError(err).Errorf("update shards of SN%d: %d", snID, index)
				// 				innerErr = &err
				// 			} else {
				// 				entry.Infof("%d shards rebuilt: %d", len(m), index)
				// 			}
				// 		}
				// 	}
				// }()
				// go func() {
				// 	//delete blocks
				// 	defer wg.Done()
				// 	if len(resp.BlockDel) > 0 {
				// 		err := cli.tikvCli.DeleteBlocks(ctx, resp.BlockDel)
				// 		if err != nil {
				// 			entry.WithError(err).Errorf("update shards of SN%d", snID)
				// 			innerErr = &err
				// 		} else {
				// 			entry.Infof("%d shards delete", len(resp.BlockDel))
				// 		}
				// 	}
				// }()
				// wg.Wait()
				// if innerErr != nil {
				// 	time.Sleep(time.Duration(cli.WaitTime) * time.Second)
				// 	continue
				// }
				err = cli.tikvCli.InsertCheckPoint(ctx, &CheckPoint{ID: snID, Start: resp.Next, Timestamp: time.Now().Unix()})
				if err != nil {
					entry.WithError(err).Errorf("update checkpoint of SN%d", snID)
					time.Sleep(time.Duration(cli.WaitTime) * time.Second)
					continue
				}
				entry.Debugf("update checkpoint to %d", resp.Next)
				cli.lock.Lock()
				checkPoints[snID] = (resp.Next >> 32) * 1000000000
				cli.lock.Unlock()
				if !resp.More {
					time.Sleep(time.Duration(cli.WaitTime) * time.Second)
				}
			}
		}()
	}
	return nil
}

//GetSyncData find synchronization data
func GetSyncData(httpCli *http.Client, url string, from int64, size int, skip int) (*DataResp, error) {
	entry := log.WithFields(log.Fields{Function: "GetSyncData"})
	fullURL := fmt.Sprintf("%s/sync/getSyncData?from=%d&size=%d&skip=%d", url, from, size, skip)
	entry.Debugf("fetching sync data by URL: %s", fullURL)
	request, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		entry.WithError(err).Errorf("create request failed: %s", fullURL)
		return nil, err
	}
	request.Header.Add("Accept-Encoding", "gzip")
	resp, err := httpCli.Do(request)
	if err != nil {
		entry.WithError(err).Errorf("get sync data failed: %s", fullURL)
		return nil, err
	}
	defer resp.Body.Close()
	reader := io.Reader(resp.Body)
	if strings.Contains(resp.Header.Get("Content-Encoding"), "gzip") {
		gbuf, err := gzip.NewReader(reader)
		if err != nil {
			entry.WithError(err).Errorf("decompress response body: %s", fullURL)
			return nil, err
		}
		reader = io.Reader(gbuf)
		defer gbuf.Close()
	}
	response := new(DataResp)
	err = json.NewDecoder(reader).Decode(response)
	if err != nil {
		entry.WithError(err).Errorf("decode sync data failed: %s", fullURL)
		return nil, err
	}
	// sort.Slice(response.Shards[:], func(i, j int) bool {
	// 	return response.Shards[i].NodeID < response.Shards[j].NodeID
	// })
	return response, nil
}

//GetCheckPoints fetch checkpoints
func (cli *Client) GetCheckPoints(c echo.Context) error {
	entry := log.WithFields(log.Fields{Function: "GetCheckPoints"})
	cli.lock.RLock()
	b, err := json.Marshal(checkPoints)
	cli.lock.RUnlock()
	if err != nil {
		entry.WithError(err).Error("marshaling data to json")
		return c.String(http.StatusInternalServerError, err.Error())
	}
	return c.JSONBlob(http.StatusOK, b)
}

//GetCheckPoints fetch checkpoints
func (cli *Client) GetBlocks(c echo.Context) error {
	entry := log.WithFields(log.Fields{Function: "GetBlocks"})
	bindexesStr := c.QueryParam("bindexes")
	bindexesArr := strings.Split(bindexesStr, ",")
	bindexes := make([]uint64, 0)
	for _, s := range bindexesArr {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			entry.WithError(err).Errorf("parse bindex failed: %s", s)
			return c.String(http.StatusInternalServerError, err.Error())
		}
		bindexes = append(bindexes, n)
	}
	dblocks, err := cli.arraybase.Read(bindexes)
	if err != nil {
		entry.WithError(err).Error("read blocks failed")
		return c.String(http.StatusInternalServerError, err.Error())
	}
	// blocks := make([]*ytab.Block, 0)
	// for _, i := range bindexes {
	// 	blocks = append(blocks, dblocks[i])
	// }
	b, err := json.Marshal(dblocks)
	if err != nil {
		entry.WithError(err).Error("marshaling data to json")
		return c.String(http.StatusInternalServerError, err.Error())
	}
	return c.JSONBlob(http.StatusOK, b)
}
