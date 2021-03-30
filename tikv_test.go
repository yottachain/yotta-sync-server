package ytsync

import (
	"context"
	"fmt"
	"testing"

	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
)

func TestTikv(t *testing.T) {
	cli, err := rawkv.NewClient(context.TODO(), []string{"127.0.0.1:2379"}, config.Default())
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	fmt.Printf("cluster ID: %d\n", cli.ClusterID())

	// keys := make([][]byte, 0)
	// keys = append(keys, []byte("blocks_1"))
	// keys = append(keys, []byte("blocks_12"))
	// keys = append(keys, []byte("blocks_5"))

	// vals, err := cli.BatchGet(context.TODO(), keys)
	// if err != nil {
	// 	panic(err)
	// }
	// for _, v := range vals {
	// 	fmt.Printf("found val: %s\n", v)
	// }

	// for _, v := range []int{2, 1, 4, 3, 6, 5, 8, 7, 9} {
	// 	key := []byte(fmt.Sprintf("blocks_%d", v))
	// 	val := []byte(fmt.Sprintf("shards_%d", v))
	// 	// put key into tikv
	// 	err = cli.Put(context.TODO(), key, val)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	fmt.Printf("Successfully put %s:%s to tikv\n", key, val)
	// }
	// keys, values, err := cli.ReverseScan(context.TODO(), append([]byte("blocks_6"), '\x00'), append([]byte("blocks_0"), '\x00'), 4)
	// if err != nil {
	// 	panic(err)
	// }
	// for i := 0; i < len(keys); i++ {
	// 	k := keys[i]
	// 	v := values[i]
	// 	fmt.Printf("found val: %s for key: %s\n", v, k)
	// }
	key := []byte("Company")
	// val := []byte("PingCAP")

	// // put key into tikv
	// err = cli.Put(context.TODO(), key, val)
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("Successfully put %s:%s to tikv\n", key, val)

	// get key from tikv
	val, err := cli.Get(context.TODO(), key)
	if err != nil {
		panic(err)
	}
	if val == nil {
		fmt.Printf("not found val")
	}
	fmt.Printf("found val: %s for key: %s\n", val, key)

	// // delete key from tikv
	// err = cli.Delete(context.TODO(), key)
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("key: %s deleted\n", key)

	// // get key again from tikv
	// val, err = cli.Get(context.TODO(), key)
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("found val: %s for key: %s\n", val, key)
}
