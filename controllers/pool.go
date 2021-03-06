package controllers

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ivpusic/grpool"
	"gopkg.in/mgo.v2/bson"
)

// type Pool struct {
// 	blocks *[]Block
// }

//Executor 参数传递
type Executor struct {
	Snid            int
	AddURL          string
	TimeC           int
	SleepTime       int
	Pool            *grpool.Pool
	dao             *Dao
	blocks          []Block
	ShardRebuidMeta []ShardRebuidMeta
}

func (executor *Executor) start(addr string, start, end int32, snid int) {
	// executor.PullBlocksAndShardsByTimes(addr, snid)

	// executor.Pool.JobQueue <- func() {
	// 	bs := executor.blocks
	// 	executor.InsertBlockAndShard(bs)
	// }

	executor.blocks = executor.PullBlocksAndShards(addr, start, end, snid)
	bs := executor.blocks
	executor.InsertBlockAndShard(bs)
	// executor.Pool.JobQueue <- func() {
	// 	bs := executor.blocks
	// 	executor.InsertBlockAndShard(bs)
	// }
}

func (executor *Executor) update(addr string, start, end int32, snid int) {
	executor.ShardRebuidMeta = executor.PullShardRebuidMetas(addr, start, end, snid)
	bs := executor.ShardRebuidMeta
	executor.UpdateShards(bs)

}

//PullBlocksAndShardsByTimes old pull
func (executor *Executor) PullBlocksAndShardsByTimes(snAttrs string, sn int) {

	fmt.Println("Goroutine ", sn)
	sess := executor.dao.client[0].Copy()
	defer sess.Close()
	t := sess.DB("metabase").C("record")
	r := new(Record)
	t.Find(bson.M{"sn": r.Sn}).Sort("-1").Limit(1).One(r)
	start := fmt.Sprintf("%d", r.StartTime)
	end := fmt.Sprintf("%d", r.EndTime)
	now1 := time.Now().Unix() - int64(executor.TimeC)
	endTime, err := strconv.ParseInt(end, 10, 32)
	if err != nil {

	}
	if now1 < int64(endTime) {
		// 比较时间戳，如果发现当前时间比查询的endTime值小，让程序休眠10分钟继续
		fmt.Println("同步结束时间大于系统时间，程序进入休眠状态，自动唤醒时间：", executor.TimeC, " 分钟后")
		time.Sleep(time.Minute * time.Duration(executor.TimeC))
	}

	var blocks []Block
	fmt.Println("snAttrs:", snAttrs, " ,start:", start, ",end:", end, ",sn:", sn)

	//生成要访问的url
	url := snAttrs + "/sync/get_blocks?start=" + start + "&end=" + end

	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("sn", sn, ",Error getting sn data  ", snAttrs)

	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err == nil {
		err = json.Unmarshal(body, &blocks)
	}

	// 屏蔽写表功能
	executor.blocks = blocks
	executor.Pool.JobQueue <- func() {
		bs := executor.blocks
		executor.InsertBlockAndShard(bs)
	}
	fmt.Println("blocks len:::::::::::::::::::", len(blocks))
	record := Record{}
	entTime, err3 := strconv.ParseInt(end, 10, 32)
	CheckErr(err3)
	time1 := executor.dao.cfg.GetRecieveInfo("time")
	time32, err4 := strconv.ParseInt(time1, 10, 32)
	CheckErr(err4)
	min32 := int32(entTime)
	max32 := int32(entTime) + int32(time32)

	record.StartTime = min32
	record.EndTime = max32
	record.Sn = sn
	selector := bson.M{"sn": record.Sn}
	data := bson.M{"start": record.StartTime, "end": record.EndTime, "sn": record.Sn}
	err5 := t.Update(selector, data)
	if err5 != nil {
		fmt.Println(err5)
	}
	fmt.Println("startTime ：", start, "endTime :", end, "sync sn: sn :", sn, " next ready")

}

//PullBlocksAndShards 同步块和分片
func (executor *Executor) PullBlocksAndShards(snAttrs string, startTime, endTime int32, sn int) []Block {
	fmt.Println("Goroutine ", sn)

	startc := fmt.Sprintf("%d", startTime)
	endc := fmt.Sprintf("%d", endTime)
	sess := executor.dao.client[0].Copy()
	defer sess.Close()
	t := sess.DB("metabase").C("record")
	r := new(Record)
	t.Find(bson.M{"sn": r.Sn}).Sort("-1").Limit(1).One(r)

	now1 := time.Now().Unix() - int64(executor.TimeC)

	if now1 < int64(endTime) {
		// 比较时间戳，如果发现当前时间比查询的endTime值小，让程序休眠10分钟继续
		fmt.Println("同步结束时间大于系统时间，程序进入休眠状态，自动唤醒时间：", executor.SleepTime, " 分钟后")
		time.Sleep(time.Minute * time.Duration(executor.SleepTime))
	}

	var blocks []Block
	fmt.Println("snAttrs:", snAttrs, " ,start:", startTime, ",end:", endTime, ",sn:", sn)
	time11 := time.Now().UnixNano()
	fmt.Println("Pull start,", "SN:", sn, "pull start time:", time11)
	//生成要访问的url
	url := snAttrs + "/sync/get_blocks?start=" + startc + "&end=" + endc

	resp, err := http.Get(url)

	if err != nil {
		fmt.Println("sn", sn, ",Error getting sn data  ", snAttrs)
		var retry int = 0
		for retry != 5 {
			resp, err = http.Get(url)
			if err != nil {
				time.Sleep(time.Minute * time.Duration(executor.SleepTime))
			} else {
				retry = 5
			}
		}

	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err == nil {
		err = json.Unmarshal(body, &blocks)
	}
	time22 := time.Now().UnixNano()
	fmt.Println("Pull complete,", "Block size:", len(blocks), ",SN:", sn, ",pull end time:", time22, ",pull data take times:", (time22-time11)/100000, "ms")
	// fmt.Println("blocks len:::::::::::::::::::", len(blocks))
	record := Record{}
	entTime, err3 := strconv.ParseInt(endc, 10, 32)
	CheckErr(err3)
	time1 := executor.dao.cfg.GetRecieveInfo("time")
	time32, err4 := strconv.ParseInt(time1, 10, 32)
	CheckErr(err4)
	min32 := int32(entTime)
	max32 := int32(entTime) + int32(time32)

	record.StartTime = min32
	record.EndTime = max32
	if record.StartTime == 0 {
		startTime1 := executor.dao.cfg.GetRecieveInfo("start")
		reStartTime, _ := strconv.ParseInt(startTime1, 10, 32)
		record.StartTime = int32(reStartTime)
		record.EndTime = int32(reStartTime) + int32(time32)
	}
	record.Sn = sn
	selector := bson.M{"sn": record.Sn}
	data := bson.M{"start": record.StartTime, "end": record.EndTime, "sn": record.Sn}
	err5 := t.Update(selector, data)
	if err5 != nil {
		fmt.Println("update record failure ", err5)
	}

	time33 := time.Now().UnixNano()
	fmt.Println("update record complete,", "SN:", sn, ",update record complete time:", time33, ",update record take times:", (time33-time22)/100000, "ms")
	return blocks
}

//PullShardRebuidMetas 拉取重建分片数据
func (executor *Executor) PullShardRebuidMetas(snAttrs string, startTime, endTime int32, sn int) []ShardRebuidMeta {
	fmt.Println("Goroutine ", sn)

	startc := fmt.Sprintf("%d", startTime)
	endc := fmt.Sprintf("%d", endTime)
	sess := executor.dao.client[0].Copy()
	defer sess.Close()
	t := sess.DB("metabase").C(shardRecord)
	r := new(ShardRecord)
	t.Find(bson.M{"sn": r.Sn}).Sort("-1").Limit(1).One(r)

	now1 := time.Now().Unix() - int64(executor.TimeC)

	if now1 < int64(endTime) {
		// 比较时间戳，如果发现当前时间比查询的endTime值小，让程序休眠10分钟继续
		fmt.Println("同步结束时间大于系统时间，程序进入休眠状态，自动唤醒时间：", executor.SleepTime, " 分钟后")
		time.Sleep(time.Minute * time.Duration(executor.SleepTime))
	}

	var shardRebuidMetas []ShardRebuidMeta
	fmt.Println("snAttrs:", snAttrs, " ,start:", startTime, ",end:", endTime, ",sn:", sn)
	time11 := time.Now().UnixNano()
	fmt.Println("Pull start,", "SN:", sn, "pull start time:", time11)
	//生成要访问的url
	url := snAttrs + "/sync/getShardRebuidMetas?start=" + startc + "&end=" + endc

	resp, err := http.Get(url)

	if err != nil {
		fmt.Println("sn", sn, ",Error getting sn data  ", snAttrs)
		var retry int = 0
		for retry != 5 {
			resp, err = http.Get(url)
			if err != nil {
				time.Sleep(time.Minute * time.Duration(executor.SleepTime))
			} else {
				retry = 5
			}
		}

	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err == nil {
		err = json.Unmarshal(body, &shardRebuidMetas)
	}
	time22 := time.Now().UnixNano()
	fmt.Println("Pull complete,", "ShardRebuidMeta size:", len(shardRebuidMetas), ",SN:", sn, ",pull end time:", time22, ",pull data take times:", (time22-time11)/100000, "ms")
	// fmt.Println("blocks len:::::::::::::::::::", len(blocks))
	record := ShardRecord{}
	entTime, err3 := strconv.ParseInt(endc, 10, 32)
	CheckErr(err3)
	time1 := executor.dao.cfg.GetRecieveInfo("time")
	time32, err4 := strconv.ParseInt(time1, 10, 32)
	CheckErr(err4)
	min32 := int32(entTime)
	max32 := int32(entTime) + int32(time32)

	record.StartTime = min32
	record.EndTime = max32
	if record.StartTime == 0 {
		startTime1 := executor.dao.cfg.GetRecieveInfo("start")
		reStartTime, _ := strconv.ParseInt(startTime1, 10, 32)
		record.StartTime = int32(reStartTime)
		record.EndTime = int32(reStartTime) + int32(time32)
	}
	record.Sn = sn
	selector := bson.M{"sn": record.Sn}
	data := bson.M{"start": record.StartTime, "end": record.EndTime, "sn": record.Sn}
	err5 := t.Update(selector, data)
	if err5 != nil {
		fmt.Println("update record failure ", err5)
	}

	time33 := time.Now().UnixNano()
	fmt.Println("update ShardRecord complete,", "SN:", sn, ",update ShardRecord complete time:", time33, ",update ShardRecord take times:", (time33-time22)/100000, "ms")
	return shardRebuidMetas
}

//UpdateShards 更新Shard数据
func (executor *Executor) UpdateShards(shardRebuidMetas []ShardRebuidMeta) {
	r := rand.Intn(len(executor.dao.client))
	sess := executor.dao.client[r].Copy()
	defer sess.Close()
	s := sess.DB(metabase).C(shards)
	if len(shardRebuidMetas) > 0 {
		for _, shardRebuidMeta := range shardRebuidMetas {
			log.Printf("更新重建后分片信息，分片的ID:%d,更新前矿机ID:%d ,更新前后矿机ID:%d\n", shardRebuidMeta.VFI, shardRebuidMeta.OldNodeId, shardRebuidMeta.NewNodeId)
			selector := bson.M{"_id": shardRebuidMeta.VFI, "nodeId": shardRebuidMeta.OldNodeId}
			data := bson.M{"$set": bson.M{"nodeId": shardRebuidMeta.NewNodeId}}
			// data := bson.M{"nodeId": shardRebuidMeta.NewNodeId}
			err := s.Update(selector, data)
			if err != nil {
				log.Printf("重建数据更新失败,ShardID:  %+v\n", shardRebuidMeta.VFI)
			}
		}

	}
}

//InsertBlockAndShard 插入block shard
func (executor *Executor) InsertBlockAndShard(blocks []Block) {
	r := rand.Intn(len(executor.dao.client))
	sess := executor.dao.client[r].Copy()
	defer sess.Close()
	c := sess.DB(metabase).C("blocks")
	s := sess.DB(metabase).C("shards")
	if len(blocks) > 0 {
		time44 := time.Now().UnixNano()
		fmt.Println("SN:", executor.Snid, ",Blocks len:", len(blocks), ",time1:", time44)
		var itemsBlocks []interface{}
		for _, b := range blocks {
			n := Block{}
			n.ID = b.ID
			n.AR = b.AR
			n.VNF = b.VNF
			n.SnID = executor.Snid
			itemsBlocks = append(itemsBlocks, n)
		}
		errB := c.Insert(itemsBlocks...)

		if errB != nil {
			fmt.Println("Insert Blocks error:::", errB)

			errBStr := errB.Error()
			if !strings.ContainsAny(errBStr, "duplicate key error") {
				log.Printf("Block: Sync: error when inserting block to database: %s\n", errB.Error())
			}
		}
		time55 := time.Now().UnixNano()
		fmt.Println("Insert Blocks complete,", "SN:", executor.Snid, ",time2:", time55, ",Insert blocks take times:", (time55-time44)/100000, "ms")
		var items []interface{}
		for _, bb := range blocks {
			b := Block{}
			b.ID = bb.ID
			b.AR = bb.AR
			b.VNF = bb.VNF
			for _, ss := range bb.Shards {
				ss.BlockID = b.ID
				// sds = append(sds, ss)
				items = append(items, ss)
			}
		}

		//对shards进行排序

		sort.Sort(ShardSlice(items))

		// fmt.Printf("items: %+v\n", items)
		// for _, sdddd := range items {
		// 	fmt.Println("NodeID=", sdddd.(*Shard).NodeID)
		// }

		time66 := time.Now().UnixNano()
		fmt.Println("Insert Shards before,", "SN:", executor.Snid, ",time3:", time66, ",Shards len:", len(items), ",take times:", (time66-time55)/100000, "ms")
		errS := s.Insert(items...)
		if errS != nil {
			fmt.Println("Insert Shards error:::", errS)
			errSStr := errS.Error()
			if !strings.ContainsAny(errSStr, "duplicate key error") {
				log.Printf("Shard: Sync: error when inserting shard to database: %s\n", errS.Error())
			}
		}
		time77 := time.Now().UnixNano()
		fmt.Println("Insert Shards complete,", "SN:", executor.Snid, ",time4:", time66, "Shards len:", len(items), ",Insert shards take times:", (time77-time66)/100000, "ms")
	}
}

type ShardSlice []interface{}

func (a ShardSlice) Len() int { // 重写 Len() 方法
	return len(a)
}
func (a ShardSlice) Swap(i, j int) { // 重写 Swap() 方法
	a[i], a[j] = a[j], a[i]
}
func (a ShardSlice) Less(i, j int) bool { // 重写 Less() 方法， 升序排序
	return a[j].(*Shard).NodeID > a[i].(*Shard).NodeID
}

// saveBlocksToFile 将拉取的数据保存到本地
func (executor *Executor) saveBlocksToFile(start string, snID int, blocks []Block) {
	//创建或者打开文件
	path := "/data/sn/" + fmt.Sprintf("%d", snID) + "/" + start
	file, e := os.Create(path)
	if e != nil {
		fmt.Println("创建文件失败！", e)
	}

	//生成文件编码器
	encoder := json.NewEncoder(file)
	//使用编码器将结构体编码到文件中
	encode := encoder.Encode(blocks)
	if encode != nil {
		fmt.Println("blocks写入文件失败！")
	}
	file.Close()

}
