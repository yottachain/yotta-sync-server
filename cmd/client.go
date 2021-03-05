package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	ytsync "github.com/yottachain/yotta-sync-server"
)

// clientCmd represents the client command
var clientCmd = &cobra.Command{
	Use:   "client",
	Short: "client side of yotta-sync-server",
	Long:  `this command will synchronize meta data from remote server to local database.`,
	Run: func(cmd *cobra.Command, args []string) {
		config := new(ytsync.Config)
		if err := viper.Unmarshal(config); err != nil {
			panic(fmt.Sprintf("unable to decode into config struct, %v\n", err))
		}
		initLog(config)
		client, err := ytsync.NewClient(context.Background(), config.Client.TiDBURL, config.Client.MaxOpenConns, config.Client.MaxIdleConns, config.Client.AllSyncURLs, config.Client.StartTime, config.Client.BatchSize, config.Client.WaitTime, config.Client.SkipTime)
		if err != nil {
			panic(fmt.Sprintf("fatal error when creating synchronization client: %s\n", err))
		}
		err = client.StartClient(context.Background())
		if err != nil {
			panic(fmt.Sprintf("fatal error when starting synchronization client: %s\n", err))
		}
		c := make(chan os.Signal)
		signal.Notify(c, os.Interrupt, os.Kill)
		s := <-c
		fmt.Println("service finished, received", s)
	},
}

func init() {
	rootCmd.AddCommand(clientCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// clientCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// clientCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
