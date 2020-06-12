package controllers

import (
	"fmt"
	"log"
	"strconv"

	"github.com/yottachain/yotta-sync-server/conf"
	"gopkg.in/mgo.v2"
)

const (
	metabase = "metabase"
	shards   = "shards"
	blocks   = "blocks"
	record   = "record"
)

type Dao struct {
	client []*mgo.Session
	cfg    *conf.Config
}

func InitDao(mongoURL string, cfg *conf.Config) (*Dao, error) {
	sessions := make([]*mgo.Session, 0)
	fmt.Println("mongoURL::::", mongoURL)
	countstr := cfg.GetConfigInfo("clientCount")
	count, err := strconv.Atoi(countstr)
	if err != nil {
		panic(err)
	}
	for i := 0; i < count; i++ {
		session, err := mgo.Dial(mongoURL)
		// cli, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURL))
		if err != nil {
			log.Printf("error when creating mongodb client: %s %s\n", mongoURL, err.Error())
			return nil, err
		}
		sessions = append(sessions, session)
	}
	return &Dao{client: sessions, cfg: cfg}, nil
}
