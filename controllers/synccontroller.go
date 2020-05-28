package controllers

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/yottachain/yotta-sync-server/conf"
	"gopkg.in/mgo.v2/bson"
)

//DB 定义mongo连接
// type DB struct {
// 	Mgo *mongo.Database
// }

// 根据文件名，段名，键名获取ini的值
// func getValue(expectKey string) string {
// 	cfg, err := ini.Load("../conf/yotta_config.yaml")

// 	if err == nil {

// 	}
// 	url := cfg.Section("mongo").Key(expectKey).String()
// 	fmt.Println("url:::", url)
// 	return url
// }

//ConnecBlocksToDB 查询用户表
// func (dao *Dao) ConnecBlocksToDB() *mgo.Collection {

// 	url := conf.GetConfigInfo("url")
// 	db := conf.GetConfigInfo("db")
// 	fmt.Println("db.....", db)
// 	fmt.Println("url.....", url)
// 	session, err := mgo.Dial(url)
// 	if err != nil {
// 		panic(err)
// 	}
// 	//defer session.Close()
// 	session.SetMode(mgo.Monotonic, true)
// 	c := session.DB(db).C("blocks")
// 	return c
// }

// //ConnectRecieveBlocksToDB 连接接收端数据库 block
// func ConnectRecieveBlocksToDB() *mgo.Collection {
// 	url := conf.GetRecieveInfo("url")
// 	db := conf.GetRecieveInfo("db")
// 	fmt.Println("receive db.....", db)
// 	fmt.Println("receive url.....", url)
// 	session, err := mgo.Dial(url)
// 	if err != nil {
// 		panic(err)
// 	}
// 	//defer session.Close()
// 	session.SetMode(mgo.Monotonic, true)
// 	c := session.DB(db).C("blocks")
// 	return c
// }

// //ConnectRecieveRecordToDB 连接接收端数据库 block
// func ConnectRecieveRecordToDB() *mgo.Collection {
// 	url := conf.GetRecieveInfo("url")
// 	db := conf.GetRecieveInfo("db")
// 	fmt.Println("receive db.....", db)
// 	fmt.Println("receive url.....", url)
// 	session, err := mgo.Dial(url)
// 	if err != nil {
// 		panic(err)
// 	}
// 	//defer session.Close()
// 	session.SetMode(mgo.Monotonic, true)
// 	c := session.DB(db).C("record")
// 	return c
// }

// // ConnectShardsToDB 连接Shard表
// func ConnectShardsToDB() *mgo.Collection {
// 	session, err := mgo.Dial(conf.GetConfigInfo("url"))
// 	if err != nil {
// 		panic(err)
// 	}
// 	//defer session.Close()
// 	session.SetMode(mgo.Monotonic, true)
// 	c := session.DB(conf.GetConfigInfo("db")).C("shards")
// 	return c
// }

// //ConnectReceiveShardsToDB 连接接收端Shard表
// func ConnectReceiveShardsToDB() *mgo.Collection {
// 	session, err := mgo.Dial(conf.GetRecieveInfo("url"))
// 	if err != nil {
// 		panic(err)
// 	}
// 	//defer session.Close()
// 	session.SetMode(mgo.Monotonic, true)
// 	c := session.DB(conf.GetRecieveInfo("db")).C("shards")
// 	return c
// }

//GetBlocksByTimes 按时间段查询blocks表
func (dao *Dao) GetBlocksByTimes(g *gin.Context) {
	// c := ConnecBlocksToDB()
	// s := ConnectShardsToDB()
	c := dao.client.DB(metabase).C(blocks)
	s := dao.client.DB(metabase).C(shards)
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
	fmt.Println("min64:", min64)
	fmt.Println("max64:", max64)
	c.Find(bson.M{"_id": bson.M{"$lte": max64, "$gt": min64}}).Sort("_id").All(&blocks)
	// fmt.Println("blocks count::", len(blocks))
	size := len(blocks) - 1

	//此时间单位内最小的分片ID 和分块ID一样
	blockMinID := blocks[0].ID
	blockMaxID := blocks[size].ID
	blockMaxVNF := blocks[size].VNF - 1
	shardMaxID := blockMaxID + int64(blockMaxVNF)
	fmt.Println("min shard id:::", blockMinID)
	fmt.Println("max shard id:::", shardMaxID)
	s.Find(bson.M{"_id": bson.M{"$gte": blockMinID, "$lte": shardMaxID}}).Sort("_id").All(&shards)
	var num int
	var count int
	var ccc int
	if len(blocks) > 0 {
		for m, Block := range blocks {
			fmt.Println("Block ID------>M:", Block.ID)

			var shardAll []*Shard
			VNF := Block.VNF
			num = int(VNF)
			if m == 0 {
				fmt.Println("First Shard ID:", shards[m].ID)
				ccc = num
			} else {
				ccc = ccc + num
				fmt.Println("cccccccccc", ccc)
			}

			for i := 0; i < num; i++ {
				// shards[count].BlockID = Block.ID
				fmt.Println("count::::", count)
				shardAll = append(shardAll, &shards[count])

				count++
			}
			Block.Shards = shardAll
			result = append(result, Block)
		}

		fmt.Println("blocks total counts : ", len(blocks))
		fmt.Println("Shards total counts : ", len(shards))
	}

	g.JSON(200, result)
}

//GetShardsByBlockIDAndVNF 根据blockid、VNF查shards表
func (dao *Dao) GetShardsByBlockIDAndVNF(g *gin.Context) {
	c := dao.client.DB(metabase).C(shards)
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
	c := dao.client.DB(metabase).C(blocks)
	s := dao.client.DB(metabase).C(shards)
	t := dao.client.DB(metabase).C(record)
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
func (dao *Dao) CreateInitSyncRecord(g *gin.Context) {
	start := dao.cfg.GetRecieveInfo("start")
	time := dao.cfg.GetRecieveInfo("time")

	min, err := strconv.ParseInt(start, 10, 32)
	time32, err := strconv.ParseInt(time, 10, 32)
	max := min + time32
	CheckErr(err)
	min32 := int32(min)
	max32 := int32(max)

	c := dao.client.DB(metabase).C(record)
	for i := 0; i < 5; i++ {
		record := Record{}
		record.StartTime = min32
		record.EndTime = max32
		record.Sn = i
		c.Insert(&record)
	}

	g.String(200, "初始化record表")
}

//insertBlocksAndShardsFromService 通过传递要请求的服务器地址，开始时间结束时间 以及sn同步数据并记录record
func (dao *Dao) insertBlocksAndShardsFromService(snAttrs, start, end string, sn int) {
	c := dao.client.DB(metabase).C(blocks)
	s := dao.client.DB(metabase).C(shards)
	t := dao.client.DB(metabase).C(record)
	var blocks []Block
	fmt.Println("snAttrs:", snAttrs, " ,start:", start, ",end:", end, ",sn:", sn)

	//生成要访问的url
	url := snAttrs + "/sync/get_blocks?start=" + start + "&end=" + end

	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("sn", sn, ",Error getting sn data  ", snAttrs)
		return
	}

	fmt.Println("url::::", url)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err == nil {
		err = json.Unmarshal(body, &blocks)
	}
	if len(blocks) > 0 {
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
				fmt.Println("Insert Block error，BlockID:", bb.ID)
			}
			for _, ss := range bb.Shards {
				items = append(items, ss)
			}
			err2 := s.Insert(items...)
			if err2 != nil {
				fmt.Println(err2)
				fmt.Println("Insert shards error，blockID:", bb.ID)
			}

		}
	}
	record := Record{}
	// startTime, err := strconv.ParseInt(start, 10, 32)
	entTime, err := strconv.ParseInt(end, 10, 32)
	CheckErr(err)
	time := dao.cfg.GetRecieveInfo("time")
	time32, err := strconv.ParseInt(time, 10, 32)
	min32 := int32(entTime)
	max32 := int32(entTime) + int32(time32)

	record.StartTime = min32
	record.EndTime = max32
	record.Sn = sn
	selector := bson.M{"sn": record.Sn}
	data := bson.M{"start": record.StartTime, "end": record.EndTime, "sn": record.Sn}
	err3 := t.Update(selector, data)
	if err3 != nil {
		fmt.Println(err3)
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

	if err != nil {

	}

	var result []*Record
	num := int(countnum)

	c := dao.client.DB(metabase).C(record)
	for i := 0; i < num; i++ {
		r := new(Record)
		c.Find(bson.M{"sn": i}).Sort("-1").Limit(1).One(r)
		result = append(result, r)
	}
	time := cfg.GetRecieveInfo("time")
	time32, err := strconv.ParseInt(time, 10, 32)
	for _, record := range result {
		r := record
		go func() {
			mm := 0
			for num > 0 {
				fmt.Println("Goroutine ", r.Sn)
				var start string
				var end string
				if mm == 0 {
					start = fmt.Sprintf("%d", r.EndTime)
					end = fmt.Sprintf("%d", r.EndTime+int32(time32))
				} else {
					c.Find(bson.M{"sn": r.Sn}).Sort("-1").Limit(1).One(r)
					start = fmt.Sprintf("%d", r.StartTime)
					end = fmt.Sprintf("%d", r.EndTime)
				}

				addr := cfg.GetRecieveInfo("addrs" + fmt.Sprintf("%d", r.Sn))
				dao.insertBlocksAndShardsFromService(addr, start, end, r.Sn)
				mm++
			}
			wg.Done()
		}()
		wg.Add(1)
	}
}
