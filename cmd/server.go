/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	ytsync "github.com/yottachain/yotta-sync-server"
)

// serverCmd represents the server command
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "server side of yotta-sync-server",
	Long:  `this command will launch this service as source data supplying service.`,
	Run: func(cmd *cobra.Command, args []string) {
		config := new(ytsync.Config)
		if err := viper.Unmarshal(config); err != nil {
			panic(fmt.Sprintf("unable to decode into config struct, %v\n", err))
		}
		initLog(config)
		svr, err := ytsync.NewServer(context.Background(), config.Server.MongoDBURL, config.Server.DBName, config.Server.SNID)
		if err != nil {
			panic(fmt.Sprintf("fatal error when creating synchronization service: %s\n", err))
		}
		err = svr.StartServer(config.Server.BindAddr)
		if err != nil {
			panic(fmt.Sprintf("fatal error when starting synchronization service: %s\n", err))
		}
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// serverCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// serverCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
