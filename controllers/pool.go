package controllers

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/ivpusic/grpool"
	"gopkg.in/mgo.v2/bson"
)

// type Pool struct {
// 	blocks *[]Block
// }

type Executor struct {
	// StartTime string
	// EndTime   string
	Snid      int
	AddURL    string
	TimeC     int
	SleepTime int
	Pool      *grpool.Pool
	dao       *Dao
	blocks    []Block
}

func (executor *Executor) start(addr string, snid int) {
	executor.PullBlocksAndShardsByTimes(addr, snid)
	// executor.Pool.JobQueue <- func() {
	// 	bs := executor.blocks
	// 	executor.InsertBlockAndShard(bs)
	// }
}

// func (executor *Executor) InitPoolTask() {
// 	// number of workers, and size of job queue
// 	// pool := grpool.NewPool(10, 10)

// 	// release resources used by pool
// 	// defer pool.Release()
// 	executor.Pool.JobQueue <- func() {
// 		bs := executor.blocks
// 		// var bs []Block
// 		executor.InsertBlockAndShard(bs)
// 	}

// }

func (executor *Executor) PullBlocksAndShardsByTimes(snAttrs string, sn int) {
	var num int = 1

	for num > 0 {

		fmt.Println("Goroutine ", sn)
		t := executor.dao.client[0].DB("metabase").C("record")
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
			fmt.Println("同步结束时间大于系统时间，程序进入休眠状态，自动唤醒时间：", executor.SleepTime, " 分钟后")
			time.Sleep(time.Minute * time.Duration(executor.SleepTime))
		}

		var blocks []Block

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
		fmt.Println("Blocks size:", len(blocks))

		// fmt.Println("blocks len::XX:::::::::::::::::", len(blocks))
		record := Record{}
		entTime, err3 := strconv.ParseInt(end, 10, 32)
		CheckErr(err3)
		time1 := executor.dao.cfg.GetRecieveInfo("time")
		time32, err4 := strconv.ParseInt(time1, 10, 32)
		CheckErr(err4)
		rr := new(Record)
		t.Find(bson.M{"sn": r.Sn}).Sort("-1").Limit(1).One(rr)
		if int32(entTime) < rr.EndTime {
			record.StartTime = rr.EndTime
			record.EndTime = rr.EndTime + int32(time32)
		} else {
			min32 := int32(entTime)
			max32 := int32(entTime) + int32(time32)

			record.StartTime = min32
			record.EndTime = max32
		}

		record.Sn = sn
		selector := bson.M{"sn": record.Sn}
		data := bson.M{"start": record.StartTime, "end": record.EndTime, "sn": record.Sn}
		err5 := t.Update(selector, data)
		if err5 != nil {
			fmt.Println(err5)
		}

		// fmt.Println("snAttrs:", snAttrs, " ,start:", start, ",end:", end, ",sn:", sn,"Blocks size:",len(blocks))
		// 屏蔽写表功能
		fmt.Println("now time:", now1, "snAttrs:", snAttrs, " ,start:", start, ",end:", end, ",sn:", sn, "Blocks size:", len(blocks))

		executor.blocks = blocks
		executor.Pool.JobQueue <- func() {
			bs := executor.blocks
			// var bs []Block
			executor.InsertBlockAndShard(bs)
			// executor.saveBlocksToFile(start, executor.Snid, bs)
		}

		// fmt.Println("startTime ：", start, "endTime :", end, "sync sn: sn :", sn, " next ready")
	}

}

func (executor *Executor) InsertBlockAndShard(blocks []Block) {
	// fmt.Println("*********************************************")
	r := rand.Intn(len(executor.dao.client))
	c := executor.dao.client[r].DB(metabase).C("blocks")
	s := executor.dao.client[r].DB(metabase).C("shards")
	if len(blocks) > 0 {
		var itemsBlocks []interface{}
		for _, b := range blocks {
			n := Block{}
			n.ID = b.ID
			n.AR = b.AR
			n.VNF = b.VNF
			itemsBlocks = append(itemsBlocks, n)
		}
		errB := c.Insert(itemsBlocks...)
		if errB != nil {
			fmt.Println("Insert Blocks error")
			// return
		}
		var items []interface{}
		for _, bb := range blocks {
			b := Block{}
			b.ID = bb.ID
			b.AR = bb.AR
			b.VNF = bb.VNF

			for _, ss := range bb.Shards {
				ss.BlockID = b.ID
				items = append(items, ss)
			}

		}
		errS := s.Insert(items...)
		if errS != nil {
			fmt.Println(errS)
		}
	}
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
