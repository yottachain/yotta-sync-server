package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	ytsync "github.com/yottachain/yotta-sync-server"
	"github.com/yottachain/yotta-sync-server/cmd"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	cmd.Execute()
}

func main0() {
	const Int64Max int64 = 9223372036854775807
	nodeID, err := strconv.Atoi(os.Args[1])
	limit, err := strconv.Atoi(os.Args[2])
	skip, err := strconv.Atoi(os.Args[3])
	from, err := strconv.ParseInt(os.Args[4], 10, 64)
	if err != nil {
		panic(err)
	}
	cli, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://127.0.0.1:27020/?connect=direct"))
	if err != nil {
		panic(err)
	}
	rTab := cli.Database("rebuilder").Collection("RebuildShard")
	bTab := cli.Database("metabase").Collection("blocks")
	sTab := cli.Database("metabase").Collection("shards")
	nTab := cli.Database("rebuilder").Collection("Node")
	opt := new(options.FindOptions)

	limit64 := int64(limit)
	opt.Limit = &limit64
	opt.Sort = bson.M{"_id": -1}
	opt.Skip = &from
	cur, err := rTab.Find(context.Background(), bson.M{"minerID": nodeID, "timestamp": bson.M{"$lt": Int64Max}}, opt)
	if err != nil {
		panic(err)
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		shard := new(RebuildShard)
		err := cur.Decode(shard)
		if err != nil {
			panic(err)
		}
		block := new(ytsync.Block)
		err = bTab.FindOne(context.Background(), bson.M{"_id": shard.BlockID}).Decode(block)
		if err != nil {
			panic(err)
		}
		cur, err := sTab.Find(context.Background(), bson.M{"_id": bson.M{"$gte": block.ID, "$lt": block.ID + int64(block.VNF)}})
		if err != nil {
			panic(err)
		}
		valid := 0
		j := 0
		ne := "n"
		ids := make([]int, 0)
		nids := make([]int, 0)
		bset := make([]byte, 0)
		for cur.Next(context.Background()) {
			s := new(ytsync.Shard)
			err := cur.Decode(s)
			if err != nil {
				panic(err)
			}
			n := new(Node)
			err = nTab.FindOne(context.Background(), bson.M{"_id": s.NodeID}).Decode(n)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					ne = "y"
					nids = append(nids, int(s.NodeID))
					bset = append(bset, 0)
				} else {
					panic(err)
				}
			} else {
				if n.Timestamp > time.Now().Unix()-int64(skip) {
					valid++
					bset = append(bset, 1)
				} else {
					ids = append(ids, int(n.ID))
					bset = append(bset, 0)
				}
			}
			j++
		}
		cur.Close(context.Background())
		sort.Ints(ids)
		sort.Ints(nids)
		fmt.Printf("\"%d\", %d%%, %d, %d, %s, %v, %v, %v\n", shard.ID, int32(valid)*100.0/block.VNF, valid, j, ne, nids, ids, bset)
	}
}

func main1() {
	nodeID, err := strconv.Atoi(os.Args[1])
	limit, err := strconv.Atoi(os.Args[2])
	skip, err := strconv.Atoi(os.Args[3])
	from, err := strconv.ParseInt(os.Args[4], 10, 64)
	if err != nil {
		panic(err)
	}
	cli, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://127.0.0.1:27020/?connect=direct"))
	if err != nil {
		panic(err)
	}
	bTab := cli.Database("metabase").Collection("blocks")
	sTab := cli.Database("metabase").Collection("shards")
	nTab := cli.Database("rebuilder").Collection("Node")
	opt := new(options.FindOptions)

	limit64 := int64(limit)
	opt.Limit = &limit64
	opt.Sort = bson.M{"_id": -1}
	opt.Skip = &from
	cur, err := sTab.Find(context.Background(), bson.M{"nodeId": nodeID}, opt)
	if err != nil {
		panic(err)
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		shard := new(ytsync.Shard)
		err := cur.Decode(shard)
		if err != nil {
			panic(err)
		}
		block := new(ytsync.Block)
		err = bTab.FindOne(context.Background(), bson.M{"_id": shard.BlockID}).Decode(block)
		if err != nil {
			panic(err)
		}
		cur, err := sTab.Find(context.Background(), bson.M{"_id": bson.M{"$gte": block.ID, "$lt": block.ID + int64(block.VNF)}})
		if err != nil {
			panic(err)
		}
		valid := 0
		j := 0
		ne := "n"
		ids := make([]int, 0)
		nids := make([]int, 0)
		bset := make([]byte, 0)
		for cur.Next(context.Background()) {
			s := new(ytsync.Shard)
			err := cur.Decode(s)
			if err != nil {
				panic(err)
			}
			n := new(Node)
			err = nTab.FindOne(context.Background(), bson.M{"_id": s.NodeID}).Decode(n)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					ne = "y"
					nids = append(nids, int(s.NodeID))
					bset = append(bset, 0)
				} else {
					panic(err)
				}
			} else {
				if n.Timestamp > time.Now().Unix()-int64(skip) {
					valid++
					bset = append(bset, 1)
				} else {
					ids = append(ids, int(n.ID))
					bset = append(bset, 0)
				}
			}
			j++
		}
		cur.Close(context.Background())
		sort.Ints(ids)
		sort.Ints(nids)
		fmt.Printf("\"%d\", %d%%, %d, %d, %s, %v, %v, %v\n", shard.ID, int32(valid)*100.0/block.VNF, valid, j, ne, nids, ids, bset)
	}
}

type RebuildShard struct {
	ID               int64  `bson:"_id"`
	VHF              []byte `bson:"VHF"`
	MinerID          int32  `bson:"minerID"`
	BlockID          int64  `bson:"blockID"`
	Type             int32  `bson:"type"`
	VNF              int32  `bson:"VNF"`
	ParityShardCount int32  `bson:"parityShardCount"`
	SNID             int32  `bson:"snID"`
	Timestamp        int64  `bson:"timestamp"`
	ErrCount         int32  `bson:"errCount"`
}

type Node struct {
	//data node index
	ID int32 `bson:"_id"`
	//data node ID, generated from PubKey
	NodeID string `bson:"nodeid"`
	//public key of data node
	PubKey string `bson:"pubkey"`
	//owner account of this miner
	Owner string `bson:"owner"`
	//profit account of this miner
	ProfitAcc string `bson:"profitAcc"`
	//ID of associated miner pool
	PoolID string `bson:"poolID"`
	//Owner of associated miner pool
	PoolOwner string `bson:"poolOwner"`
	//quota allocated by associated miner pool
	Quota int64 `bson:"quota"`
	//listening addresses of data node
	Addrs []string `bson:"addrs"`
	//CPU usage of data node
	CPU int32 `bson:"cpu"`
	//memory usage of data node
	Memory int32 `bson:"memory"`
	//bandwidth usage of data node
	Bandwidth int32 `bson:"bandwidth"`
	//max space of data node
	MaxDataSpace int64 `bson:"maxDataSpace"`
	//space assigned to YTFS
	AssignedSpace int64 `bson:"assignedSpace"`
	//pre-allocated space of data node
	ProductiveSpace int64 `bson:"productiveSpace"`
	//used space of data node
	UsedSpace int64 `bson:"usedSpace"`
	//used spaces on each SN
	Uspaces map[string]int64 `bson:"uspaces"`
	//weight for allocate data node
	Weight float64 `bson:"weight"`
	//Is node valid
	Valid int32 `bson:"valid"`
	//Is relay node
	Relay int32 `bson:"relay"`
	//status code: 0 - registered 1 - active
	Status int32 `bson:"status"`
	//timestamp of status updating operation
	Timestamp int64 `bson:"timestamp"`
	//version number of miner
	Version int32 `bson:"version"`
	//Rebuilding if node is under rebuilding
	Rebuilding int32 `bson:"rebuilding"`
	//RealSpace real space of miner
	RealSpace int64 `bson:"realSpace"`
	//Tx
	Tx int64 `bson:"tx"`
	//Rx
	Rx int64 `bson:"rx"`
	//Ext
	Ext string `bson:"-"`
	//ErrorCount
	ErrorCount int32 `bson:"errorCount"`
}
