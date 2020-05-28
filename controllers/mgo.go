package controllers

import (
	"log"

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
	client *mgo.Session
	cfg    *conf.Config
}

func InitDao(mongoURL string, cfg *conf.Config) (*Dao, error) {
	session, err := mgo.Dial(mongoURL)
	// cli, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURL))
	if err != nil {
		log.Printf("error when creating mongodb client: %s %s\n", mongoURL, err.Error())
		return nil, err
	}
	return &Dao{client: session, cfg: cfg}, nil
}
