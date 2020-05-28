package main

import (
	"flag"
	"fmt"
	"os/exec"
	"runtime"
	"sync"
	"time"

	"github.com/go-ini/ini"
	"github.com/prometheus/common/log"
	"github.com/robfig/cron"
	"github.com/yottachain/yotta-sync-server/conf"
	"github.com/yottachain/yotta-sync-server/controllers"
	"github.com/yottachain/yotta-sync-server/routers"
	"github.com/yottachain/yotta-sync-server/utils"
)

func main() {
	log.Info(time.Now().Format("2006-01-02 15:04:05") + "strart ......")
	service := conf.GetRecieveInfo("service")

	var wg sync.WaitGroup
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
	log.Info("   strart ......")
	fmt.Println("off" == service)
	if service == "off" {
		fmt.Println("Start Thread service ..............")
		controllers.RunService(wg)
		wg.Wait()
	}

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
		crontab.AddFunc("*/200 * * * *", myfunc) //5S
		crontab.Start()
	}()
}

// 加个定时器
func myfunc() {
	fmt.Println("Hello,GoLang！！")
}
