package main

import (
	"flag"
	"fmt"
	"os/exec"
	"runtime"
	"time"

	"github.com/go-ini/ini"
	"github.com/prometheus/common/log"
	"github.com/robfig/cron"
	"github.com/yottachain/yotta-sync-server/routers"
	"github.com/yottachain/yotta-sync-server/utils"
)

func main() {
	log.Info(time.Now().Format("2006-01-02 15:04:05") + "strart ......")
	flag.Parse()

	cfg, err := ini.Load("../conf/yotta_config.yaml")
	if err != nil {
		panic(err)
	}

	utils.Config = cfg

	router := routers.InitRouter()
	cronInit()

	port := cfg.Section("http").Key("port").String()
	err1 := router.Run(port)
	if err1 != nil {
		panic(err1)
	}
	log.Info("strart ......")

}

var commands = map[string]string{
	"windows": "cmd /c start",
	"darwin":  "open",
	"linux":   "xdg-open",
}

func OpenUrl(uri string) {
	run, _ := commands[runtime.GOOS]
	exec.Command(run, uri).Start()
}

//定时器
func cronInit() {
	go func() {
		crontab := cron.New()
		crontab.AddFunc("*/20 * * * *", myfunc) //5S
		crontab.Start()
	}()
}

// 加个定时器
func myfunc() {
	fmt.Println("5秒打印一次！！")
}
