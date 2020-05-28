package db

import (
	"context"
	"time"

	"github.com/yottachain/yotta-sync-server/utils"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const dbName = "metabase"

func InitMongoDB2() (collection *mongo.Database) {
	if mgo == nil {
		mgo = connectDB()
	}
	return mgo
}

var mgo *mongo.Database

func connectDB() (collection *mongo.Database) {

	config := utils.GetConfInfo("mongo")

	opts := options.ClientOptions{Hosts: []string{config.Key("url").String()}}
	credential := options.Credential{
		AuthSource: config.Key("db").String(),
	}
	opts.Auth = &credential

	client, err := mongo.NewClient(&opts)
	if err != nil {
		panic(err)
		// return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		return nil
	}
	var dbName = utils.GetConf("mongo", "db")
	collection = client.Database(dbName)
	return collection
}

const (
	User  string = "User"
	Node  string = "Node"
	Test  string = "test"
	Test1 string = "test1"
	Test2 string = "test2"
)
