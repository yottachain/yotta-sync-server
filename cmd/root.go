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
	//DefaultServerBindAddrSecure default value of ServerBindAddrSecure
	//DefaultServerBindAddrSecure string = ":8487"
	//DefaultServerCertPath default value of ServerCertPath
	//DefaultServerCertPath string = "cert.pem"
	//DefaultServerKeyPath default value of ServerKeyPath
	//DefaultServerKeyPath string = "key.pem"
	//DefaultServerMongoDBURL default value of ServerMongoDBURL
	DefaultServerMongoDBURL string = "mongodb://127.0.0.1:27017/?connect=direct"
	//DefaultServerDBName default value of ServerDBName
	DefaultServerDBName string = "metabase"
	//DefaultServerMinerDBName default value of ServerMinerDBName
	DefaultServerMinerDBName string = "yotta"
	//DefaultServerSNID default value of ServerSNID
	DefaultServerSNID int = 0
	//DefaultServerSkipTime default value of ServerSkipTime
	DefaultServerSkipTime int = 180

	//DefaultClientBindAddr default value of ClientBindAddr
	DefaultClientBindAddr string = ":8087"
	//DefaultClientBindAddrSecure default value of ClientBindAddrSecure
	//DefaultClientBindAddrSecure string = ":8487"
	//DefaultClientCertPath default value of ClientCertPath
	//DefaultClientCertPath string = "cert.pem"
	//DefaultClientKeyPath default value of ClientKeyPath
	//DefaultClientKeyPath string = "key.pem"
	//DefaultClientPDURLs default value of ClientPDURLs
	DefaultClientPDURLs []string = []string{}
	//DefaultESConfigURLs default value of ESConfigURLs
	DefaultESConfigURLs []string = []string{"http://127.0.0.1:9200"}
	//DefaultESConfigUserName default value of ESConfigUserName
	DefaultESConfigUserName string = ""
	//DefaultESConfigPassword default value of ESConfigPassword
	DefaultESConfigPassword string = ""
	//DefaultESConfigEnable default value of ESConfigEnable
	DefaultESConfigEnable bool = false
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
	//DefaultClientArrayBaseBaseDir default value of ClientArrayBaseBaseDir
	DefaultClientArrayBaseBaseDir string = ""
	//DefaultClientArrayBaseRowsPerFile default value of ClientArrayBaseRowsPerFile
	DefaultClientArrayBaseRowsPerFile uint64 = 10000000
	//DefaultClientArrayBaseReadBufLen default value of ClientArrayBaseReadBufLen
	DefaultClientArrayBaseReadBufLen int = 10
	//DefaultClientArrayBaseWriteBufLen default value of ClientArrayBaseWriteBufLen
	DefaultClientArrayBaseWriteBufLen int = 10

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
	// rootCmd.PersistentFlags().String(ytsync.ServerBindAddrSecureField, DefaultServerBindAddrSecure, "Binding address of synchronization https server")
	// viper.BindPFlag(ytsync.ServerBindAddrSecureField, rootCmd.PersistentFlags().Lookup(ytsync.ServerBindAddrSecureField))
	// rootCmd.PersistentFlags().String(ytsync.ServerCertPathField, DefaultServerCertPath, "cert path of synchronization https server")
	// viper.BindPFlag(ytsync.ServerCertPathField, rootCmd.PersistentFlags().Lookup(ytsync.ServerCertPathField))
	// rootCmd.PersistentFlags().String(ytsync.ServerKeyPathField, DefaultServerKeyPath, "key path of synchronization https server")
	// viper.BindPFlag(ytsync.ServerKeyPathField, rootCmd.PersistentFlags().Lookup(ytsync.ServerKeyPathField))
	rootCmd.PersistentFlags().String(ytsync.ServerMongoDBURLField, DefaultServerMongoDBURL, "URL of Source mongoDB")
	viper.BindPFlag(ytsync.ServerMongoDBURLField, rootCmd.PersistentFlags().Lookup(ytsync.ServerMongoDBURLField))
	rootCmd.PersistentFlags().String(ytsync.ServerDBNameField, DefaultServerDBName, "name of source database")
	viper.BindPFlag(ytsync.ServerDBNameField, rootCmd.PersistentFlags().Lookup(ytsync.ServerDBNameField))
	rootCmd.PersistentFlags().String(ytsync.ServerMinerDBNameField, DefaultServerMinerDBName, "name of source miner database")
	viper.BindPFlag(ytsync.ServerMinerDBNameField, rootCmd.PersistentFlags().Lookup(ytsync.ServerMinerDBNameField))
	rootCmd.PersistentFlags().Int(ytsync.ServerSNIDField, DefaultServerSNID, "SN index")
	viper.BindPFlag(ytsync.ServerSNIDField, rootCmd.PersistentFlags().Lookup(ytsync.ServerSNIDField))
	rootCmd.PersistentFlags().Int(ytsync.ServerSkipTimeField, DefaultServerSkipTime, "ensure not to fetching stored shards till the end")
	viper.BindPFlag(ytsync.ServerSkipTimeField, rootCmd.PersistentFlags().Lookup(ytsync.ServerSkipTimeField))
	//client config
	rootCmd.PersistentFlags().String(ytsync.ClientBindAddrField, DefaultClientBindAddr, "Binding address of synchronization client http server")
	viper.BindPFlag(ytsync.ClientBindAddrField, rootCmd.PersistentFlags().Lookup(ytsync.ClientBindAddrField))
	// rootCmd.PersistentFlags().String(ytsync.ClientBindAddrSecureField, DefaultClientBindAddrSecure, "Binding address of synchronization client https server")
	// viper.BindPFlag(ytsync.ClientBindAddrSecureField, rootCmd.PersistentFlags().Lookup(ytsync.ClientBindAddrSecureField))
	// rootCmd.PersistentFlags().String(ytsync.ClientCertPathField, DefaultClientCertPath, "cert path of synchronization client https server")
	// viper.BindPFlag(ytsync.ClientCertPathField, rootCmd.PersistentFlags().Lookup(ytsync.ClientCertPathField))
	// rootCmd.PersistentFlags().String(ytsync.ClientKeyPathField, DefaultClientKeyPath, "key path of synchronization client https server")
	// viper.BindPFlag(ytsync.ClientKeyPathField, rootCmd.PersistentFlags().Lookup(ytsync.ClientKeyPathField))
	rootCmd.PersistentFlags().StringSlice(ytsync.ClientPDURLsField, DefaultClientPDURLs, "URLs of PD")
	viper.BindPFlag(ytsync.ClientPDURLsField, rootCmd.PersistentFlags().Lookup(ytsync.ClientPDURLsField))
	//ES config
	rootCmd.PersistentFlags().StringSlice(ytsync.ESURLsField, DefaultESConfigURLs, "URLs of ES")
	viper.BindPFlag(ytsync.ESURLsField, rootCmd.PersistentFlags().Lookup(ytsync.ESURLsField))
	rootCmd.PersistentFlags().String(ytsync.ESUserNameField, DefaultESConfigUserName, "username of elasticsearch")
	viper.BindPFlag(ytsync.ESUserNameField, rootCmd.PersistentFlags().Lookup(ytsync.ESUserNameField))
	rootCmd.PersistentFlags().String(ytsync.ESPasswordField, DefaultESConfigPassword, "password of elasticsearch")
	viper.BindPFlag(ytsync.ESPasswordField, rootCmd.PersistentFlags().Lookup(ytsync.ESPasswordField))
	rootCmd.PersistentFlags().Bool(ytsync.ESEnableField, DefaultESConfigEnable, "whether enable sync mechanism on elasticsearch")
	viper.BindPFlag(ytsync.ESEnableField, rootCmd.PersistentFlags().Lookup(ytsync.ESEnableField))
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
	rootCmd.PersistentFlags().String(ytsync.ClientArrayBaseBaseDirField, DefaultClientArrayBaseBaseDir, "base directory of arraybase")
	viper.BindPFlag(ytsync.ClientArrayBaseBaseDirField, rootCmd.PersistentFlags().Lookup(ytsync.ClientArrayBaseBaseDirField))
	rootCmd.PersistentFlags().Uint64(ytsync.ClientArrayBaseRowsPerFileField, DefaultClientArrayBaseRowsPerFile, "rows count per file of arraybase")
	viper.BindPFlag(ytsync.ClientArrayBaseRowsPerFileField, rootCmd.PersistentFlags().Lookup(ytsync.ClientArrayBaseRowsPerFileField))
	rootCmd.PersistentFlags().Int(ytsync.ClientArrayBaseReadBufLenField, DefaultClientArrayBaseReadBufLen, "read buffer length of arraybase")
	viper.BindPFlag(ytsync.ClientArrayBaseReadBufLenField, rootCmd.PersistentFlags().Lookup(ytsync.ClientArrayBaseReadBufLenField))
	rootCmd.PersistentFlags().Int(ytsync.ClientArrayBaseWriteBufLenField, DefaultClientArrayBaseWriteBufLen, "write buffer length of arraybase")
	viper.BindPFlag(ytsync.ClientArrayBaseWriteBufLenField, rootCmd.PersistentFlags().Lookup(ytsync.ClientArrayBaseWriteBufLenField))
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
