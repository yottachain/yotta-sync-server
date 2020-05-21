package controllers

import (
	"bytes"
	"encoding/binary"
	"fmt"
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

//ConnecShardsToDB 查询矿机表
func ConnecShardsToDB() *mgo.Collection {
	session, err := mgo.Dial(conf.GetConfigInfo("url"))
	if err != nil {
		panic(err)
	}
	//defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	c := session.DB(conf.GetConfigInfo("db")).C("shards")
	return c
}

//GetBlocksByTimes 按时间段查询blocks表
func (m DB) GetBlocksByTimes(g *gin.Context) {
	c := ConnecBlocksToDB()
	s := ConnecShardsToDB()
	var result []Block
	var shards []Shard
	var msg []Msg
	minID := g.Query("minID")
	maxID := g.Query("maxID")
	min, err := strconv.ParseInt(minID, 10, 32)
	max, err := strconv.ParseInt(maxID, 10, 32)
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
	c.Find(bson.M{"_id": bson.M{"$lt": max64, "$gte": min64}}).All(&result)
	fmt.Println("多少条", len(result))
	for _, Block := range result {

		mm := Msg{}
		mm.ID = Block.ID
		mm.VNF = Block.VNF
		mm.AR = Block.AR
		// var length int
		VNF := Block.VNF
		var maxShardId int64
		maxShardId = Block.ID + int64(VNF)
		// length = int(VNF)
		s.Find(bson.M{"_id": bson.M{"$gte": Block.ID, "$lte": maxShardId}}).All(&shards)
		// for i := 0; i < length; i++ {
		// 	//根据VNF的值遍历Shards,
		// 	shard := Shard{}
		// 	shard.ID = Block.ID + int64(i)
		// 	s.Find(bson.M{"_id": shard.ID}).One(&shard)

		// 	// fmt.Println("shards：：", shard)
		// 	shards = append(shards, shard)
		// }
		// mm.Shards = shards
		mm.Shards = shards
		msg = append(msg, mm)
	}

	g.JSON(200, msg)
}

//GetShardsByBlockIDAndVNF 根据blockid、VNF查shards表
func (m DB) GetShardsByBlockIDAndVNF(g *gin.Context) {
	c := ConnecShardsToDB()
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
