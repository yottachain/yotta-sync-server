package controllers

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ivpusic/grpool"
	"github.com/yottachain/yotta-sync-server/conf"
	"gopkg.in/mgo.v2/bson"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

//GetIDByTimestamp 根据时间戳获取ID最小范围
func (dao *Dao) GetIDByTimestamp(g *gin.Context) {
	timec := g.Query("time")
	timeINT, err := strconv.ParseInt(timec, 10, 32)
	CheckErr(err)
	time32 := int32(timeINT)
	timeByte := Int32ToBytes(time32)
	ee := []byte{0x00, 0x00, 0x00, 0x00}
	data := [][]byte{timeByte, ee}
	datas := bytes.Join(data, []byte{})
	ID := BytesToInt64(datas)
	fmt.Println("ID:::::", ID)
	g.String(200, "ID=%s", fmt.Sprintf("%d", ID))
}

func (dao *Dao) GetTimeStamp(g *gin.Context) {
	blockID := g.Query("blockID")
	fmt.Println("blockID::::::::::", blockID)
	block64, err := strconv.ParseInt(blockID, 10, 64)

	CheckErr(err)
	byteBlockID32 := Int64ToBytes(block64)[:4]

	blockID32 := BytesToInt32(byteBlockID32)
	timeStamp := fmt.Sprintf("%d", blockID32)
	g.String(200, "时间戳："+timeStamp)
}

func (dao *Dao) GetBlocksByTimes(g *gin.Context) {
	metabase_db := dao.cfg.GetConfigInfo("db")

	r := rand.Intn(len(dao.client))
	c := dao.client[r].DB(metabase_db).C(blocks)
	s := dao.client[r].DB(metabase_db).C(shards)
	var blocks []Block
	var shards []Shard
	var result []Block
	// messages := Messages{}
	start := g.Query("start")
	end := g.Query("end")
	min, err := strconv.ParseInt(start, 10, 32)
	max, err := strconv.ParseInt(end, 10, 32)
	CheckErr(err)
	min32 := int32(min)
	max32 := int32(max)
	//将时间戳转byte
	minbyte := Int32ToBytes(min32)
	maxbyte := Int32ToBytes(max32)
	ee := []byte{0x00, 0x00, 0x00, 0x00}
	mindata := [][]byte{minbyte, ee}
	maxdata := [][]byte{maxbyte, ee}
	mindatas := bytes.Join(mindata, []byte{})
	maxdatas := bytes.Join(maxdata, []byte{})
	min64 := BytesToInt64(mindatas)
	max64 := BytesToInt64(maxdatas)
	// fmt.Println("min64:", min64)
	// fmt.Println("max64:", max64)
	c.Find(bson.M{"_id": bson.M{"$lte": max64, "$gt": min64}}).Sort("_id").All(&blocks)
	// fmt.Println("blocks count::", len(blocks))

	var num int
	var count int
	var ccc int
	if len(blocks) > 0 {

		size := len(blocks) - 1

		//此时间单位内最小的分片ID 和分块ID一样
		blockMinID := blocks[0].ID
		blockMaxID := blocks[size].ID
		blockMaxVNF := blocks[size].VNF - 1
		shardMaxID := blockMaxID + int64(blockMaxVNF)
		fmt.Println("min shard id:::", blockMinID)
		fmt.Println("max shard id:::", shardMaxID)
		s.Find(bson.M{"_id": bson.M{"$gte": blockMinID, "$lte": shardMaxID}}).Sort("_id").All(&shards)

		for m, Block := range blocks {
			// fmt.Println("Block ID------>M:", Block.ID)

			var shardAll []*Shard
			VNF := Block.VNF
			num = int(VNF)
			if m == 0 {
				// fmt.Println("First Shard ID:", shards[m].ID)
				ccc = num
			} else {
				ccc = ccc + num
				// fmt.Println("cccccccccc", ccc)
			}

			for i := 0; i < num; i++ {
				// shards[count].BlockID = Block.ID
				// fmt.Println("count::::", count)
				shardAll = append(shardAll, &shards[count])

				count++
			}
			Block.Shards = shardAll
			result = append(result, Block)
		}

		// fmt.Println("blocks total counts : ", len(blocks))
		// fmt.Println("Shards total counts : ", len(shards))
	}

	fmt.Println(min64, max64, "blocks size:::", len(result), "shards size:::", len(shards))
	g.JSON(200, result)
}

//GetShardsByBlockIDAndVNF 根据blockid、VNF查shards表
func (dao *Dao) GetShardsByBlockIDAndVNF(g *gin.Context) {
	r := rand.Intn(len(dao.client))
	c := dao.client[r].DB(metabase).C(shards)
	var result []Shard
	blockIDStr := g.Query("blockID")
	blockID, err := strconv.ParseInt(blockIDStr, 10, 64)
	VNFSTR := g.Query("VNF")
	VNF, err := strconv.ParseInt(VNFSTR, 10, 32)
	CheckErr(err)

	var length int
	length = int(VNF)
	if length > 0 {
		for i := 0; i < length; i++ {
			//根据VNF的值遍历Shards,
			shard := Shard{}
			var id int64
			id = blockID + int64(i)
			fmt.Println("id:::::", id)
			c.Find(bson.M{"_id": id}).One(&shard)

			result = append(result, shard)
		}
	}

	fmt.Println("Block:", blockID, " Shards Total count ", len(result), " shards")
	g.JSON(200, result)
}

//CheckErr 检查错误原因
func CheckErr(err error) {
	if err != nil {
		panic(err)
	}
}

//Int64ToBytes 将int转byte
func Int64ToBytes(n int64) []byte {
	data := int64(n)
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.BigEndian, data)
	return bytebuf.Bytes()
}

//Int32ToBytes 将int转byte
func Int32ToBytes(n int32) []byte {
	data := int32(n)
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.BigEndian, data)
	return bytebuf.Bytes()
}

//BytesToInt32 将byte转int32
func BytesToInt32(bys []byte) int32 {
	bytebuff := bytes.NewBuffer(bys)
	var data int32
	binary.Read(bytebuff, binary.BigEndian, &data)
	return data
}

//BytesToInt64 将byte转int64
func BytesToInt64(bys []byte) int64 {
	bytebuff := bytes.NewBuffer(bys)
	var data int64
	binary.Read(bytebuff, binary.BigEndian, &data)
	return data
}

//ReceiveInfo 远程请求接收方返回test
func (dao *Dao) ReceiveInfo(g *gin.Context) {
	r := rand.Intn(len(dao.client))
	c := dao.client[r].DB(metabase).C(blocks)
	s := dao.client[r].DB(metabase).C(shards)
	t := dao.client[r].DB(metabase).C(record)
	// messages := Messages{}
	var blocks []Block
	record := Record{}
	// c.Find(bson.M{"_id": i32,"poolOwner":poolOwner}).One(&node)
	t.Find(bson.M{"sn": 0}).Sort("-1").Limit(1).One(&record)
	fmt.Println("record查到最新的开始时间：", record.StartTime)
	fmt.Println("record查到最新的结束时间：", record.EndTime)
	time := dao.cfg.GetRecieveInfo("time")
	start := strconv.Itoa(int(record.EndTime))
	timee, err := strconv.Atoi(time)
	end := strconv.Itoa(int(record.EndTime) + timee)
	// start := g.Query("start")
	// end := g.Query("end")
	// client := &http.Client{}

	//获取服务端的请求url
	addrs := dao.cfg.GetRecieveInfo("addrs")

	fmt.Println("addrs:::::", addrs)

	//生成要访问的url
	url := addrs + "/sync/get_blocks?start=" + start + "&end=" + end

	resp, err := http.Get(url)
	if err != nil {
		// handle error
	}

	fmt.Println("url::::", url)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err == nil {
		err = json.Unmarshal(body, &blocks)
	}
	for _, bb := range blocks {
		var items []interface{}
		// fmt.Println("blockssdsfjsjkdfs", bb.ID)
		b := Block{}
		b.ID = bb.ID
		b.AR = bb.AR
		b.VNF = bb.VNF
		err1 := c.Insert(&b)
		if err1 != nil {
			fmt.Println(err1)
			fmt.Println("接收服务器插入Block错误，BlockID:", bb.ID)
		}
		for _, ss := range bb.Shards {
			items = append(items, ss)
		}
		err2 := s.Insert(items...)
		if err2 != nil {
			fmt.Println(err2)
			fmt.Println("接收服务器批量插入分片错误，所属块ID:", bb.ID)
		}
	}

	g.JSON(200, blocks)
}

//CreateInitSyncRecord 创建初始同步记录
// func (dao *Dao) CreateInitSyncRecord(g *gin.Context) {
// 	start := dao.cfg.GetRecieveInfo("start")
// 	interval := dao.cfg.GetRecieveInfo("time")

// 	min, err := strconv.ParseInt(start, 10, 32)
// 	time32, err := strconv.ParseInt(interval, 10, 32)
// 	max := min + time32
// 	CheckErr(err)
// 	min32 := int32(min)
// 	max32 := int32(max)

// 	c := dao.client.DB(metabase).C(record)
// 	for i := 0; i < 5; i++ {
// 		record := Record{}
// 		record.StartTime = min32
// 		record.EndTime = max32
// 		record.Sn = i
// 		c.Insert(&record)
// 	}

// 	g.String(200, "初始化record表")
// }

//CreateInitRecord 创建初始同步记录
func CreateInitRecord(start, interval string, num int, dao *Dao) {
	// start := dao.cfg.GetRecieveInfo("start")
	// interval := dao.cfg.GetRecieveInfo("time")

	min, err := strconv.ParseInt(start, 10, 32)
	time32, err := strconv.ParseInt(interval, 10, 32)
	max := min + time32
	CheckErr(err)
	min32 := int32(min)
	max32 := int32(max)

	r := rand.Intn(len(dao.client))
	c := dao.client[r].DB(metabase).C(record)
	for i := 0; i < num; i++ {
		record := Record{}
		recordOld := Record{}
		record.StartTime = min32
		record.EndTime = max32
		record.Sn = i
		c.Find(bson.M{"sn": i}).One(&recordOld)
		if recordOld.StartTime == 0 {
			fmt.Println("Init record table ...", record.Sn)
			c.Insert(&record)
			fmt.Println("Init data add complete...")
		}

	}

}

//insertBlocksAndShardsFromService 通过传递要请求的服务器地址，开始时间结束时间 以及sn同步数据并记录record
func (dao *Dao) insertBlocksAndShardsFromService(snAttrs, start, end string, sn int) {
	r := rand.Intn(len(dao.client))
	c := dao.client[r].DB(metabase).C(blocks)
	s := dao.client[r].DB(metabase).C(shards)
	t := dao.client[r].DB(metabase).C(record)
	var blocks []Block
	fmt.Println("snAttrs:", snAttrs, " ,start:", start, ",end:", end, ",sn:", sn)

	//生成要访问的url
	url := snAttrs + "/sync/get_blocks?start=" + start + "&end=" + end

	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("sn", sn, ",Error getting sn data  ", snAttrs)
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err == nil {
		err = json.Unmarshal(body, &blocks)
	}
	if len(blocks) > 0 {
		fmt.Println("startBlockID:", start, "endBlockID", end, "SN:", sn, "Block counts:", len(blocks))
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
	record := Record{}
	entTime, err3 := strconv.ParseInt(end, 10, 32)
	CheckErr(err3)
	time1 := dao.cfg.GetRecieveInfo("time")
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

//RunService 启动线程函数
func RunService(wg *sync.WaitGroup, cfg *conf.Config) {
	fmt.Println("RunService.........")
	dao, err := InitDao(cfg.GetRecieveInfo("url"), cfg)
	if err != nil {
		panic(err)
	}
	sncount := cfg.GetRecieveInfo("sncount")
	countnum, err := strconv.ParseInt(sncount, 10, 32)
	sleetTime1 := cfg.GetRecieveInfo("sleep")
	sleepTime2, err := strconv.ParseInt(sleetTime1, 10, 32)
	if err != nil {

	}
	var result []*Record
	num := int(countnum)
	r := rand.Intn(len(dao.client))
	c := dao.client[r].DB(metabase).C(record)
	for i := 0; i < num; i++ {
		r := new(Record)
		c.Find(bson.M{"sn": i}).Sort("-1").Limit(1).One(r)
		result = append(result, r)
	}
	timec := cfg.GetRecieveInfo("time")
	time32, err := strconv.ParseInt(timec, 10, 32)
	coroutinesNumber1 := cfg.GetRecieveInfo("coroutinesNumber")
	coroutinesNumber32, err := strconv.ParseInt(coroutinesNumber1, 10, 32)
	coroutinesNumber := int(coroutinesNumber32)
	for _, record := range result {
		re := record
		var executor Executor
		addr := cfg.GetRecieveInfo("addrs" + fmt.Sprintf("%d", re.Sn))
		executor.AddURL = addr
		executor.TimeC = int(time32)
		executor.SleepTime = int(sleepTime2)
		executor.Snid = re.Sn
		pool := grpool.NewPool(coroutinesNumber, 0)
		executor.Pool = pool
		executor.dao = dao

		// executor.InitPoolTask()
		go func() {
			for coroutinesNumber > 0 {
				rnm := new(Record)
				c.Find(bson.M{"sn": re.Sn}).Sort("-1").Limit(1).One(rnm)
				executor.start(addr, rnm.StartTime, rnm.EndTime, executor.Snid)
			}
		}()

		// for i := 0; i < coroutinesNumber; i++ {
		// 	go func(){}
		// }
	}
	// for _, record := range result {
	// 	re := record
	// 	var executor Executor
	// 	addr := cfg.GetRecieveInfo("addrs" + fmt.Sprintf("%d", re.Sn))

	// }

	// for _, record := range result {
	// 	r := record
	// 	var ex Executor
	// 	go func() {
	// 		mm := 0
	// 		for num > 0 {
	// 			fmt.Println("Goroutine ", r.Sn)
	// 			delayTime1 := cfg.GetRecieveInfo("delayTime")
	// 			delayTime, err := strconv.ParseInt(delayTime1, 10, 32)
	// 			if err != nil {
	// 			}
	// 			now1 := time.Now().Unix() - delayTime
	// 			if now1 < int64(r.EndTime) {
	// 				// 比较时间戳，如果发现当前时间比查询的endTime值小，让程序休眠10分钟继续

	// 				sleepTime := time.Duration(sleepTime2)
	// 				fmt.Println("同步结束时间大于系统时间，程序进入休眠状态，自动唤醒时间：", sleepTime, " 分钟后")
	// 				time.Sleep(time.Minute * sleepTime)
	// 			}
	// 			var start string
	// 			var end string
	// 			if mm == 0 {
	// 				start = fmt.Sprintf("%d", r.EndTime)
	// 				end = fmt.Sprintf("%d", r.EndTime+int32(time32))
	// 			} else {
	// 				c.Find(bson.M{"sn": r.Sn}).Sort("-1").Limit(1).One(r)
	// 				start = fmt.Sprintf("%d", r.StartTime)
	// 				end = fmt.Sprintf("%d", r.EndTime)
	// 			}

	// 			addr := cfg.GetRecieveInfo("addrs" + fmt.Sprintf("%d", r.Sn))
	// 			ex.PullBlocksAndShardsByTimes(addr, start, end, r.Sn, dao)
	// 			// dao.insertBlocksAndShardsFromService(addr, start, end, r.Sn)

	// 			mm++
	// 		}
	// 		wg.Done()
	// 	}()
	// 	wg.Add(1)
	// }
}

//GetShardsCount 获取真实分片数量
func (dao *Dao) GetShardsCount(g *gin.Context) {
	metabase_db := dao.cfg.GetConfigInfo("db")

	r := rand.Intn(len(dao.client))
	c := dao.client[r].DB(metabase_db).C(blocks)
	var blocks []Block
	// messages := Messages{}
	start := g.Query("start")
	end := g.Query("end")
	min, err := strconv.ParseInt(start, 10, 64)
	max, err := strconv.ParseInt(end, 10, 64)
	CheckErr(err)

	c.Find(bson.M{"_id": bson.M{"$lte": max, "$gt": min}}).Sort("_id").All(&blocks)
	var count int32 = 0
	for _, block := range blocks {
		count = count + block.VNF
	}
	fmt.Println("count:", count)

	g.String(200, "shardsCount=%s", fmt.Sprintf("%d", count))
}
