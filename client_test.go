package ytsync

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestGetSyncData(t *testing.T) {
	data, err := GetSyncData(new(http.Client), "http://192.168.36.132:8057", 0, 10, 180)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(data.SNID)
}

func TestOrdered(t *testing.T) {
	cli, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
	if err != nil {
		t.Fatal(err)
	}
	tab := cli.Database("test").Collection("test")
	opt := new(options.InsertManyOptions)
	ordered := false
	opt.Ordered = &ordered
	arr := make([]interface{}, 0)
	arr = append(arr, map[string]int{"_id": 1, "a": 2})
	arr = append(arr, map[string]int{"_id": 3, "a": 3})
	arr = append(arr, map[string]int{"_id": 5, "a": 5})
	arr = append(arr, map[string]int{"_id": 7, "a": 7})
	arr = append(arr, map[string]int{"_id": 9, "a": 9})
	result, err := tab.InsertMany(context.Background(), arr, opt)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(result.InsertedIDs...)
}
