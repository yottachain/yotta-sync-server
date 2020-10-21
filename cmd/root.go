package cmd

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	homedir "github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	ytsync "github.com/yottachain/yotta-sync-server"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "yotta-sync-server",
	Short: "blocks and shards synchronization service of YottaChain",
	Long:  `yotta-sync-server is an synchronization service performing data synchronizing from SN.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		// config := new(ytsync.Config)
		// if err := viper.Unmarshal(config); err != nil {
		// 	panic(fmt.Sprintf("unable to decode into config struct, %v\n", err))
		// }
		// initLog(config)
		// rebuilder, err := ytsync.New(config.AnalysisDBURL, config.RebuilderDBURL, config.AuraMQ, config.Compensation, config.MiscConfig)
		// if err != nil {
		// 	panic(fmt.Sprintf("fatal error when starting rebuilder service: %s\n", err))
		// }
		// rebuilder.Start()
		// lis, err := net.Listen("tcp", config.BindAddr)
		// if err != nil {
		// 	log.Fatalf("failed to listen address %s: %s\n", config.BindAddr, err)
		// }
		// log.Infof("GRPC address: %s", config.BindAddr)
		// grpcServer := grpc.NewServer()
		// server := &ytsync.Server{Rebuilder: rebuilder}
		// pb.RegisterRebuilderServer(grpcServer, server)
		// grpcServer.Serve(lis)
		// log.Info("GRPC server started")
	},
}

func initLog(config *ytsync.Config) {
	switch strings.ToLower(config.Logger.Output) {
	case "file":
		writer, _ := rotatelogs.New(
			config.Logger.FilePath+".%Y%m%d",
			rotatelogs.WithLinkName(config.Logger.FilePath),
			rotatelogs.WithMaxAge(time.Duration(config.Logger.MaxAge)*time.Hour),
			rotatelogs.WithRotationTime(time.Duration(config.Logger.RotationTime)*time.Hour),
		)
		log.SetOutput(writer)
	case "stdout":
		log.SetOutput(os.Stdout)
	default:
		fmt.Printf("no such option: %s, use stdout\n", config.Logger.Output)
		log.SetOutput(os.Stdout)
	}
	log.SetFormatter(&log.TextFormatter{})
	levelMap := make(map[string]log.Level)
	levelMap["panic"] = log.PanicLevel
	levelMap["fatal"] = log.FatalLevel
	levelMap["error"] = log.ErrorLevel
	levelMap["warn"] = log.WarnLevel
	levelMap["info"] = log.InfoLevel
	levelMap["debug"] = log.DebugLevel
	levelMap["trace"] = log.TraceLevel
	log.SetLevel(levelMap[strings.ToLower(config.Logger.Level)])
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/yotta-sync-server.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	// rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	initFlag()
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".yotta-rebuilder" (without extension).
		viper.AddConfigPath(home)
		viper.AddConfigPath(".")
		viper.SetConfigName("yotta-sync-server")
		viper.SetConfigType("yaml")
	}

	// viper.AutomaticEnv() // read in environment variables that match
	// viper.SetEnvPrefix("analysis")
	// viper.SetEnvKeyReplacer(strings.NewReplacer("_", "."))

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	} else {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore error if desired
			fmt.Println("Config file not found.")
		} else {
			// Config file was found but another error was produced
			fmt.Println("Error:", err.Error())
			os.Exit(1)
		}
	}
}

var (
	//DefaultServerBindAddr default value of ServerBindAddr
	DefaultServerBindAddr string = ":8087"
	//DefaultServerMongoDBURL default value of ServerMongoDBURL
	DefaultServerMongoDBURL string = "mongodb://127.0.0.1:27017/?connect=direct"
	//DefaultServerDBName default value of ServerDBName
	DefaultServerDBName string = "metabase"
	//DefaultServerSNID default value of ServerSNID
	DefaultServerSNID int = 0
	//DefaultServerSkipTime default value of ServerSkipTime
	DefaultServerSkipTime int = 180

	//DefaultClientMongoDBURL default value of ClientMongoDBURL
	DefaultClientMongoDBURL string = "mongodb://127.0.0.1:27017/?connect=direct"
	//DefaultClientDBName default value of ClientDBName
	DefaultClientDBName string = "metabase"
	//DefaultClientAllSyncURLs default value of ClientAllSyncURLs
	DefaultClientAllSyncURLs []string = []string{}
	//DefaultClientStartTime default value of ClientStartTime
	DefaultClientStartTime int32 = 0
	//DefaultClientBatchSize default value of ClientBatchSize
	DefaultClientBatchSize int = 1000
	//DefaultClientWaitTime default value of ClientWaitTime
	DefaultClientWaitTime int = 60
	//DefaultClientSkipTime default value of ClientSkipTime
	DefaultClientSkipTime int = 180

	//DefaultLoggerOutput default value of LoggerOutput
	DefaultLoggerOutput string = "stdout"
	//DefaultLoggerFilePath default value of LoggerFilePath
	DefaultLoggerFilePath string = "./sync.log"
	//DefaultLoggerRotationTime default value of LoggerRotationTime
	DefaultLoggerRotationTime int64 = 24
	//DefaultLoggerMaxAge default value of LoggerMaxAge
	DefaultLoggerMaxAge int64 = 240
	//DefaultLoggerLevel default value of LoggerLevel
	DefaultLoggerLevel string = "Info"
)

func initFlag() {
	//server config
	rootCmd.PersistentFlags().String(ytsync.ServerBindAddrField, DefaultServerBindAddr, "Binding address of synchronization http server")
	viper.BindPFlag(ytsync.ServerBindAddrField, rootCmd.PersistentFlags().Lookup(ytsync.ServerBindAddrField))
	rootCmd.PersistentFlags().String(ytsync.ServerMongoDBURLField, DefaultServerMongoDBURL, "URL of Source mongoDB")
	viper.BindPFlag(ytsync.ServerMongoDBURLField, rootCmd.PersistentFlags().Lookup(ytsync.ServerMongoDBURLField))
	rootCmd.PersistentFlags().String(ytsync.ServerDBNameField, DefaultServerDBName, "name of source database")
	viper.BindPFlag(ytsync.ServerDBNameField, rootCmd.PersistentFlags().Lookup(ytsync.ServerDBNameField))
	rootCmd.PersistentFlags().Int(ytsync.ServerSNIDField, DefaultServerSNID, "SN index")
	viper.BindPFlag(ytsync.ServerSNIDField, rootCmd.PersistentFlags().Lookup(ytsync.ServerSNIDField))
	rootCmd.PersistentFlags().Int(ytsync.ServerSkipTimeField, DefaultServerSkipTime, "ensure not to fetching stored shards till the end")
	viper.BindPFlag(ytsync.ServerSkipTimeField, rootCmd.PersistentFlags().Lookup(ytsync.ServerSkipTimeField))
	//client config
	rootCmd.PersistentFlags().String(ytsync.ClientMongoDBURLField, DefaultClientMongoDBURL, "URL of destination mongoDB")
	viper.BindPFlag(ytsync.ClientMongoDBURLField, rootCmd.PersistentFlags().Lookup(ytsync.ClientMongoDBURLField))
	rootCmd.PersistentFlags().String(ytsync.ClientDBNameField, DefaultClientDBName, "name of destination database")
	viper.BindPFlag(ytsync.ClientDBNameField, rootCmd.PersistentFlags().Lookup(ytsync.ClientDBNameField))
	rootCmd.PersistentFlags().StringSlice(ytsync.ClientAllSyncURLsField, DefaultClientAllSyncURLs, "all URLs of sync services, in the form of --client.all-sync-urls \"URL1,URL2,URL3\"")
	viper.BindPFlag(ytsync.ClientAllSyncURLsField, rootCmd.PersistentFlags().Lookup(ytsync.ClientAllSyncURLsField))
	rootCmd.PersistentFlags().Int32(ytsync.ClientStartTimeField, DefaultClientStartTime, "synchronizing from this timestamp")
	viper.BindPFlag(ytsync.ClientStartTimeField, rootCmd.PersistentFlags().Lookup(ytsync.ClientStartTimeField))
	rootCmd.PersistentFlags().Int(ytsync.ClientBatchSizeField, DefaultClientBatchSize, "batch size when fetching shards that have been rebuilt")
	viper.BindPFlag(ytsync.ClientBatchSizeField, rootCmd.PersistentFlags().Lookup(ytsync.ClientBatchSizeField))
	rootCmd.PersistentFlags().Int(ytsync.ClientWaitTimeField, DefaultClientWaitTime, "wait time when no new shards rebuit can be fetched")
	viper.BindPFlag(ytsync.ClientWaitTimeField, rootCmd.PersistentFlags().Lookup(ytsync.ClientWaitTimeField))
	rootCmd.PersistentFlags().Int(ytsync.ClientSkipTimeField, DefaultClientSkipTime, "ensure not to fetching rebuilt shards till the end")
	viper.BindPFlag(ytsync.ClientSkipTimeField, rootCmd.PersistentFlags().Lookup(ytsync.ClientSkipTimeField))
	//logger config
	rootCmd.PersistentFlags().String(ytsync.LoggerOutputField, DefaultLoggerOutput, "Output type of logger(stdout or file)")
	viper.BindPFlag(ytsync.LoggerOutputField, rootCmd.PersistentFlags().Lookup(ytsync.LoggerOutputField))
	rootCmd.PersistentFlags().String(ytsync.LoggerFilePathField, DefaultLoggerFilePath, "Output path of log file")
	viper.BindPFlag(ytsync.LoggerFilePathField, rootCmd.PersistentFlags().Lookup(ytsync.LoggerFilePathField))
	rootCmd.PersistentFlags().Int64(ytsync.LoggerRotationTimeField, DefaultLoggerRotationTime, "Rotation time(hour) of log file")
	viper.BindPFlag(ytsync.LoggerRotationTimeField, rootCmd.PersistentFlags().Lookup(ytsync.LoggerRotationTimeField))
	rootCmd.PersistentFlags().Int64(ytsync.LoggerMaxAgeField, DefaultLoggerMaxAge, "Within the time(hour) of this value each log file will be kept")
	viper.BindPFlag(ytsync.LoggerMaxAgeField, rootCmd.PersistentFlags().Lookup(ytsync.LoggerMaxAgeField))
	rootCmd.PersistentFlags().String(ytsync.LoggerLevelField, DefaultLoggerLevel, "Log level(Trace, Debug, Info, Warning, Error, Fatal, Panic)")
	viper.BindPFlag(ytsync.LoggerLevelField, rootCmd.PersistentFlags().Lookup(ytsync.LoggerLevelField))
}
