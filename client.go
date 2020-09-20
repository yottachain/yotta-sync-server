package ytsync

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//Client client struct
type Client struct {
	httpCli   *http.Client
	dbCli     *mongo.Client
	dbName    string
	SyncURLs  []string
	StartTime int32
	BatchSize int
	WaitTime  int
	SkipTime  int
}

//NewClient create a new client instance
func NewClient(ctx context.Context, mongoDBURL, dbName string, syncURLs []string, startTime int32, batchSize, waitTime, skipTime int) (*Client, error) {
	entry := log.WithFields(log.Fields{Function: "NewClient"})
	dbClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoDBURL))
	if err != nil {
		entry.WithError(err).Errorf("creating mongo DB client failed: %s", mongoDBURL)
		return nil, err
	}
	entry.Infof("mongoDB connected: %s", mongoDBURL)
	return &Client{httpCli: &http.Client{}, dbCli: dbClient, dbName: dbName, SyncURLs: syncURLs, StartTime: startTime, BatchSize: batchSize, WaitTime: waitTime, SkipTime: skipTime}, nil
}

//StartClient start client service
func (cli *Client) StartClient(ctx context.Context) error {
	urls := cli.SyncURLs
	snCount := len(urls)
	blocksTab := cli.dbCli.Database(cli.dbName).Collection(BlocksTab)
	shardsTab := cli.dbCli.Database(cli.dbName).Collection(ShardsTab)
	checkPointTab := cli.dbCli.Database(cli.dbName).Collection(CheckPointTab)
	recordTab := cli.dbCli.Database(cli.dbName).Collection(RecordTab)
	for i := 0; i < snCount; i++ {
		snID := int32(i)
		go func() {
			entry := log.WithFields(log.Fields{Function: "StartClient", SNID: snID})
			entry.Info("starting synchronization process")
			for {
				checkPoint := new(CheckPoint)
				err := checkPointTab.FindOne(ctx, bson.M{"_id": snID}).Decode(checkPoint)
				if err != nil {
					if err == mongo.ErrNoDocuments {
						entry.Infof("cannot find checkpoint of SN%d, read record info...", snID)
						record := new(Record)
						err := recordTab.FindOne(ctx, bson.M{"sn": snID}).Decode(record)
						if err != nil {
							if err == mongo.ErrNoDocuments {
								entry.Infof("cannot find record of SN%d, read start time from config file...", snID)
								startBytes32 := Int32ToBytes(cli.StartTime)
								padding := []byte{0x00, 0x00, 0x00, 0x00}
								startTime64 := BytesToInt64(append(startBytes32, padding...))
								checkPoint = &CheckPoint{ID: snID, Start: startTime64, Timestamp: time.Now().Unix()}
								_, err = checkPointTab.InsertOne(context.Background(), checkPoint)
								if err != nil {
									entry.WithError(err).Errorf("insert checkpoint: %d", snID)
									time.Sleep(time.Duration(cli.WaitTime) * time.Second)
									continue
								}
								entry.Infof("insert checkpoint of SN%d: %d", snID, checkPoint.Start)
							} else {
								entry.WithError(err).Errorf("find record of SN%d", snID)
								time.Sleep(time.Duration(cli.WaitTime) * time.Second)
								continue
							}
						} else {
							entry.Infof("find record of SN%d, migrante to checkpoint table...", snID)
							startBytes32 := Int32ToBytes(record.StartTime)
							padding := []byte{0x00, 0x00, 0x00, 0x00}
							startTime64 := BytesToInt64(append(startBytes32, padding...))
							checkPoint = &CheckPoint{ID: snID, Start: startTime64, Timestamp: time.Now().Unix()}
							_, err = checkPointTab.InsertOne(context.Background(), checkPoint)
							if err != nil {
								entry.WithError(err).Errorf("insert checkpoint: %d", snID)
								time.Sleep(time.Duration(cli.WaitTime) * time.Second)
								continue
							}
							entry.Infof("migrante to checkpoint table of SN%d: %d", snID, checkPoint.Start)
						}
					} else {
						entry.WithError(err).Errorf("find checkpoint of SN%d", snID)
						time.Sleep(time.Duration(cli.WaitTime) * time.Second)
						continue
					}
				}
				resp, err := GetSyncData(cli.httpCli, cli.SyncURLs[snID], checkPoint.Start, cli.BatchSize, cli.SkipTime)
				if err != nil {
					entry.WithError(err).Errorf("Get sync data of SN%d", snID)
					time.Sleep(time.Duration(cli.WaitTime) * time.Second)
					continue
				}
				if snID != int32(resp.SNID) {
					entry.Fatalf("received SN ID not match current process: %d", resp.SNID)
				}
				entry.Debugf("received response of SN%d: %d blocks, %d shards, %d rebuilds, %s", resp.SNID, len(resp.Blocks), len(resp.Shards), len(resp.Rebuilds), func() string {
					if resp.More == true {
						return "have more data"
					}
					return "no more data"
				}())
				opt := new(options.InsertManyOptions)
				ordered := false
				opt.Ordered = &ordered
				var innerErr *error
				var wg sync.WaitGroup
				wg.Add(3)
				go func() {
					//insert blocks
					defer wg.Done()
					if len(resp.Blocks) > 0 {
						var ifBlocks []interface{}
						for _, b := range resp.Blocks {
							b.SnID = resp.SNID
							ifBlocks = append(ifBlocks, b)
						}
						result, err := blocksTab.InsertMany(ctx, ifBlocks, opt)
						if result != nil {
							entry.Infof("%d blocks inserted", len(result.InsertedIDs))
						} else if err != nil {
							entry.WithError(err).Errorf("insert blocks of SN%d", snID)
							innerErr = &err
						}
					}
				}()
				go func() {
					//insert shards
					defer wg.Done()
					if len(resp.Shards) > 0 {
						var ifShards []interface{}
						for _, s := range resp.Shards {
							ifShards = append(ifShards, s)
						}
						result, err := shardsTab.InsertMany(ctx, ifShards, opt)
						if result != nil {
							entry.Infof("%d shards inserted", len(result.InsertedIDs))
						} else if err != nil {
							entry.WithError(err).Errorf("insert shards of SN%d", snID)
							innerErr = &err
						}
					}
				}()
				go func() {
					//update rebuilt shards
					defer wg.Done()
					if len(resp.Rebuilds) > 0 {
						var i int64 = 0
						for _, r := range resp.Rebuilds {
							result, err := shardsTab.UpdateOne(ctx, bson.M{"_id": r.VFI, "nodeId": r.SID}, bson.M{"$set": bson.M{"nodeId": r.NID}})
							if result != nil {
								entry.Tracef("%d shards updated: %d", result.ModifiedCount, r.VFI)
								i += result.ModifiedCount
							} else if err != nil {
								entry.WithError(err).Errorf("update shards of SN%d: %d", snID, r.VFI)
								innerErr = &err
								return
							}
						}
						entry.Infof("%d shards rebuilt", i)
					}
				}()
				wg.Wait()
				if innerErr != nil {
					time.Sleep(time.Duration(cli.WaitTime) * time.Second)
					continue
				}
				_, err = checkPointTab.UpdateOne(ctx, bson.M{"_id": snID}, bson.M{"$set": bson.M{"start": resp.Next, "timestamp": time.Now().Unix()}})
				if err != nil {
					entry.WithError(err).Errorf("update checkpoint of SN%d", snID)
					time.Sleep(time.Duration(cli.WaitTime) * time.Second)
					continue
				}
				if resp.More == false {
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
	sort.Slice(response.Shards[:], func(i, j int) bool {
		return response.Shards[i].NodeID < response.Shards[j].NodeID
	})
	return response, nil
}
