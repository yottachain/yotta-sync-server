package utils

import "github.com/go-ini/ini"

func GetConf(name string, key string) string {
	cfg, err := ini.Load("../conf/yotta_config.yaml")
	if err != nil {
		panic(err)
	}
	return cfg.Section(name).Key(key).String()
}

var Config *ini.File

func GetConfInfo(name string) *ini.Section {
	cfg := Config
	return cfg.Section(name)
}
