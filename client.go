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
)

//Client client struct
type Client struct {
	httpCli   *http.Client
	tikvCli   *TikvDao
	SyncURLs  []string
	StartTime int32
	BatchSize int
	WaitTime  int
	SkipTime  int
}

//NewClient create a new client instance
func NewClient(ctx context.Context, pdURLs []string, syncURLs []string, startTime int32, batchSize, waitTime, skipTime int) (*Client, error) {
	entry := log.WithFields(log.Fields{Function: "NewClient"})
	tikvCli, err := NewTikvDao(ctx, pdURLs)
	if err != nil {
		entry.WithError(err).Errorf("creating tikv client failed: %v", pdURLs)
		return nil, err
	}
	return &Client{httpCli: &http.Client{}, tikvCli: tikvCli, SyncURLs: syncURLs, StartTime: startTime, BatchSize: batchSize, WaitTime: waitTime, SkipTime: skipTime}, nil
}

//StartClient start client service
func (cli *Client) StartClient(ctx context.Context) error {
	urls := cli.SyncURLs
	snCount := len(urls)
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
					} else {
						entry.WithError(err).Errorf("find checkpoint of SN%d", snID)
						time.Sleep(time.Duration(cli.WaitTime) * time.Second)
						continue
					}
				}
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
				entry.Debugf("received response of SN%d: %d blocks, %d shards, %d rebuilds, %s", resp.SNID, len(resp.Blocks), len(resp.Shards), len(resp.Rebuilds), func() string {
					if resp.More == true {
						return "have more data"
					}
					return "no more data"
				}())
				var innerErr *error
				var wg sync.WaitGroup
				wg.Add(3)
				go func() {
					defer wg.Done()
					if len(resp.Blocks) > 0 {
						for _, b := range resp.Blocks {
							b.SnID = int32(resp.SNID)
						}
						err := cli.tikvCli.InsertBlocks(ctx, resp.Blocks)
						if err != nil {
							entry.WithError(err).Errorf("insert blocks of SN%d", snID)
							innerErr = &err
						} else {
							entry.Infof("%d blocks inserted", len(resp.Blocks))
						}
					}
				}()
				go func() {
					//insert shards
					defer wg.Done()
					if len(resp.Shards) > 0 {
						err := cli.tikvCli.InsertShards(ctx, resp.Shards)
						if err != nil {
							entry.WithError(err).Errorf("insert shards of SN%d", snID)
							innerErr = &err
						} else {
							entry.Infof("%d shards inserted", len(resp.Shards))
						}
					}
				}()
				go func() {
					//update rebuilt shards
					defer wg.Done()
					if len(resp.Rebuilds) > 0 {
						err := cli.tikvCli.UpdateShards(ctx, resp.Rebuilds)
						if err != nil {
							entry.WithError(err).Errorf("update shards of SN%d", snID)
							innerErr = &err
						} else {
							entry.Infof("%d shards rebuilt", len(resp.Rebuilds))
						}
					}
				}()
				wg.Wait()
				if innerErr != nil {
					time.Sleep(time.Duration(cli.WaitTime) * time.Second)
					continue
				}
				err = cli.tikvCli.InsertCheckPoint(ctx, &CheckPoint{ID: snID, Start: resp.Next, Timestamp: time.Now().Unix()})
				if err != nil {
					entry.WithError(err).Errorf("update checkpoint of SN%d", snID)
					time.Sleep(time.Duration(cli.WaitTime) * time.Second)
					continue
				}
				entry.Debugf("update checkpoint to %d", resp.Next)
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
