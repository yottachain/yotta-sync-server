package conf

import (
	"github.com/go-ini/ini"
)

//GetConfigInfo 根据key获取配置文件相关信息
func GetConfigInfo(expectKey string) string {
	cfg, err := ini.Load("../conf/yotta_config.yaml")

	if err != nil {
		panic(err)
	}
	return cfg.Section("mongo").Key(expectKey).String()
}
