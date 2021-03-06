package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/common/log"
	"github.com/robfig/cron"
	"github.com/yottachain/yotta-sync-server/conf"
	"github.com/yottachain/yotta-sync-server/routers"
)

func main() {

	var path string
	if len(os.Args) > 1 {
		if os.Args[1] != "" {
			path = os.Args[1]
		} else {
			path = "../conf/yotta_config.ini"
		}

	} else {
		path = "../conf/yotta_config.ini"
	}

	cfg, err := conf.CreateConfig(path)
	if err != nil {
		panic(err)
	}

	//conf := &conf.Config
	log.Info(time.Now().Format("2006-01-02 15:04:05") + " start ......")
	service := cfg.GetRecieveInfo("service")
	log.Info("service::::::::", "off" == service)
	log.Info("   start ......")
	fmt.Println("service::::::::", "off" == service)

	flag.Parse()
	wg := &sync.WaitGroup{}
	router := routers.InitRouter(cfg, wg)
	// cronInit()

	port := cfg.GetHTTPInfo("port")
	err1 := router.Run(port)
	if err1 != nil {
		panic(err1)
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
	fmt.Println("Hello,Panda")
}
