package controllers

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/yottachain/yotta-sync-server/conf"
	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

//DB 定义mongo连接
type DB struct {
	Mgo *mongo.Database
}

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
func ConnecBlocksToDB() *mgo.Collection {

	url := conf.GetConfigInfo("url")
	db := conf.GetConfigInfo("db")
	fmt.Println("db.....", db)
	fmt.Println("url.....", url)
	session, err := mgo.Dial(url)
	if err != nil {
		panic(err)
	}
	//defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	c := session.DB(db).C("blocks")
	return c
}

//ConnectRecieveBlocksToDB 连接接收端数据库 block
func ConnectRecieveBlocksToDB() *mgo.Collection {
	url := conf.GetRecieveInfo("url")
	db := conf.GetRecieveInfo("db")
	fmt.Println("db.....", db)
	fmt.Println("url.....", url)
	session, err := mgo.Dial(url)
	if err != nil {
		panic(err)
	}
	//defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	c := session.DB(db).C("blocks")
	return c
}

// ConnectShardsToDB 连接Shard表
func ConnectShardsToDB() *mgo.Collection {
	session, err := mgo.Dial(conf.GetConfigInfo("url"))
	if err != nil {
		panic(err)
	}
	//defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	c := session.DB(conf.GetConfigInfo("db")).C("shards")
	return c
}

//ConnectReceiveShardsToDB 连接接收端Shard表
func ConnectReceiveShardsToDB() *mgo.Collection {
	session, err := mgo.Dial(conf.GetRecieveInfo("url"))
	if err != nil {
		panic(err)
	}
	//defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	c := session.DB(conf.GetRecieveInfo("db")).C("shards")
	return c
}

//GetBlocksByTimes 按时间段查询blocks表
func (m DB) GetBlocksByTimes(g *gin.Context) {
	c := ConnecBlocksToDB()
	s := ConnectShardsToDB()
	var blocks []Block
	var shards []Shard
	messages := Messages{}
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
	c.Find(bson.M{"_id": bson.M{"$lt": max64, "$gte": min64}}).Sort("_id").All(&blocks)
	fmt.Println("多少条", len(blocks))
	size := len(blocks) - 1

	//此时间单位内最小的分片ID 和分块ID一样
	blockMinID := blocks[0].ID
	blockMaxID := blocks[size].ID
	blockMaxVNF := blocks[size].VNF - 1
	shardMaxID := blockMaxID + int64(blockMaxVNF)
	fmt.Println("min shard id:::", blockMinID)
	fmt.Println("max shard id:::", shardMaxID)
	s.Find(bson.M{"_id": bson.M{"$gte": blockMinID, "$lte": shardMaxID}}).Sort("_id").All(&shards)
	messages.Blocks = blocks
	messages.Shards = shards

	fmt.Println("本次共查询到的分块数量为 ： ", len(blocks))
	fmt.Println("本次共查询到的分片数量为 ： ", len(shards))
	fmt.Println("shard:", shards[0])
	g.JSON(200, messages)
}

// //GetBlocksByTimes 按时间段查询blocks表
// func (m DB) GetBlocksByTimes(g *gin.Context) {
// 	c := ConnecBlocksToDB()
// 	s := ConnecShardsToDB()
// 	var result []Block
// 	var shards []Shard
// 	var msg []Msg
// 	minID := g.Query("minID")
// 	maxID := g.Query("maxID")
// 	min, err := strconv.ParseInt(minID, 10, 32)
// 	max, err := strconv.ParseInt(maxID, 10, 32)
// 	CheckErr(err)
// 	min32 := int32(min)
// 	max32 := int32(max)
// 	//将时间戳转byte
// 	minbyte := Int32ToBytes(min32)
// 	maxbyte := Int32ToBytes(max32)
// 	ee := []byte{0x00, 0x00, 0x00, 0x00}
// 	mindata := [][]byte{minbyte, ee}
// 	maxdata := [][]byte{maxbyte, ee}
// 	mindatas := bytes.Join(mindata, []byte{})
// 	maxdatas := bytes.Join(maxdata, []byte{})
// 	min64 := BytesToInt64(mindatas)
// 	max64 := BytesToInt64(maxdatas)
// 	fmt.Println("min64:", min64)
// 	fmt.Println("max64:", max64)
// 	c.Find(bson.M{"_id": bson.M{"$lt": max64, "$gte": min64}}).All(&result)
// 	fmt.Println("多少条", len(result))
// 	size := len(result) - 1

// 	//此时间单位内最小的分片ID 和分块ID一样
// 	blockMinID := result[0].ID
// 	blockMaxID := result[size].ID
// 	blockMaxVNF := result[size].VNF - 1
// 	shardMaxID := blockMaxID + int64(blockMaxVNF)
// 	fmt.Println("min shard id:::", blockMinID)
// 	fmt.Println("max shard id:::", shardMaxID)
// 	s.Find(bson.M{"_id": bson.M{"$gte": blockMinID, "$lte": shardMaxID}}).All(&shards)
// 	var num int
// 	for _, Block := range result {
// 		var shardsInBlock []Shard
// 		mm := Msg{}
// 		mm.ID = Block.ID
// 		mm.VNF = Block.VNF
// 		mm.AR = Block.AR
// 		sharesLen := int(Block.VNF)
// 		// var count int
// 		for i := 0; i < sharesLen; i++ {
// 			shardsInBlock = append(shardsInBlock, shards[num])
// 			num++

// 		}
// 		mm.Shards = shardsInBlock

// 		msg = append(msg, mm)
// 	}
// 	fmt.Println("num=====", num)

// 	g.JSON(200, msg)
// }

// //GetBlocksByTimes 按时间段查询blocks表
// func (m DB) GetBlocksByTimes(g *gin.Context) {
// 	c := ConnecBlocksToDB()
// 	s := ConnecShardsToDB()
// 	var result []Block
// 	var shards []Shard
// 	var msg []Msg
// 	minID := g.Query("minID")
// 	maxID := g.Query("maxID")
// 	min, err := strconv.ParseInt(minID, 10, 32)
// 	max, err := strconv.ParseInt(maxID, 10, 32)
// 	CheckErr(err)
// 	min32 := int32(min)
// 	max32 := int32(max)
// 	//将时间戳转byte
// 	minbyte := Int32ToBytes(min32)
// 	maxbyte := Int32ToBytes(max32)
// 	ee := []byte{0x00, 0x00, 0x00, 0x00}
// 	mindata := [][]byte{minbyte, ee}
// 	maxdata := [][]byte{maxbyte, ee}
// 	mindatas := bytes.Join(mindata, []byte{})
// 	maxdatas := bytes.Join(maxdata, []byte{})
// 	min64 := BytesToInt64(mindatas)
// 	max64 := BytesToInt64(maxdatas)
// 	fmt.Println("min64:", min64)
// 	fmt.Println("max64:", max64)
// 	c.Find(bson.M{"_id": bson.M{"$lt": max64, "$gte": min64}}).All(&result)
// 	fmt.Println("多少条", len(result))
// 	size := len(result) - 1

// 	//此时间单位内最小的分片ID 和分块ID一样
// 	blockMinID := result[0].ID
// 	blockMaxID := result[size].ID
// 	blockMaxVNF := result[size].VNF - 1
// 	shardMaxID := blockMaxID + int64(blockMaxVNF)
// 	fmt.Println("min shard id:::", blockMinID)
// 	fmt.Println("max shard id:::", shardMaxID)
// 	s.Find(bson.M{"_id": bson.M{"$gte": blockMinID, "$lte": shardMaxID}}).All(&shards)
// 	var num int
// 	for _, Block := range result {
// 		var shardsInBlock []Shard
// 		mm := Msg{}
// 		mm.ID = Block.ID
// 		mm.VNF = Block.VNF
// 		mm.AR = Block.AR
// 		sharesLen := int(Block.VNF)
// 		// var count int
// 		for i := 0; i < sharesLen; i++ {
// 			shardsInBlock = append(shardsInBlock, shards[num])
// 			num++

// 		}
// 		mm.Shards = shardsInBlock

// 		msg = append(msg, mm)
// 	}
// 	fmt.Println("num=====", num)

// 	g.JSON(200, msg)
// }

//GetShardsByBlockIDAndVNF 根据blockid、VNF查shards表
func (m DB) GetShardsByBlockIDAndVNF(g *gin.Context) {
	c := ConnectShardsToDB()
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

	fmt.Println("Block:", blockID, " 共有 ", len(result), " 个分片")
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
func (m DB) ReceiveInfo(g *gin.Context) {
	c := ConnectRecieveBlocksToDB()
	s := ConnectReceiveShardsToDB()
	messages := Messages{}
	start := g.Query("start")
	end := g.Query("end")
	// client := &http.Client{}

	//获取服务端的请求url
	adds := conf.GetRecieveInfo("adds")

	//生成要访问的url
	url := adds + "/sync/get_blocks?start=" + start + "&end=" + end

	resp, err := http.Get(url)
	if err != nil {
		// handle error
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err == nil {
		fmt.Println("dfsjlksdjksl")
		err = json.Unmarshal(body, &messages)
	}
	blocks := messages.Blocks

	shards := messages.Shards
	var count int
	var num int
	var msg []Msg
	for _, Block := range blocks {
		mm := Msg{}
		VNF := Block.VNF
		num = int(VNF)
		mm.ID = Block.ID
		mm.AR = Block.AR
		mm.VNF = Block.VNF
		var shardsInBlock []Shard
		for i := 0; i < num; i++ {
			shards[count].BlockID = Block.ID
			shardsInBlock = append(shardsInBlock, shards[count])
			count++
		}
		mm.Shards = shardsInBlock
		msg = append(msg, mm)
	}
	for _, Msg := range msg {
		var block Block
		var shardss []Shard
		block.ID = Msg.ID
		block.AR = Msg.AR
		block.VNF = Msg.VNF
		shardss = Msg.Shards
		var items []interface{}

		for _, sd := range shardss {
			items = append(items, sd)
		}
		err := c.Insert(&block)
		if err != nil {
			fmt.Println(err)
			fmt.Println("出错的分块ID:", block.ID)

		}
		fmt.Println("批量插入shards")
		errs := s.Insert(items...)
		if errs != nil {
			fmt.Println(errs)

		}
	}

	g.JSON(200, msg)
}
