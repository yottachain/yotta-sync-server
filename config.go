package ytsync

const (
	//ServerBindAddrField field name of server.bind-addr
	ServerBindAddrField = "server.bind-addr"
	//ServerMongoDBURLField field name of server.mongodb-url
	ServerMongoDBURLField = "server.mongodb-url"
	//ServerDBNameField field name of server.db-name
	ServerDBNameField = "server.db-name"
	//ServerMinerDBNameField field name of server.miner-db-name
	ServerMinerDBNameField = "server.miner-db-name"
	//ServerSNIDField field name of server.sn-id
	ServerSNIDField = "server.sn-id"
	//ServerSkipTimeField Field name of server.skip-time
	ServerSkipTimeField = "server.skip-time"

	//ClientMongoDBURLField field name of client.mongodb-url
	ClientMongoDBURLField = "client.mongodb-url"
	//ClientDBNameField field name of client.db-name
	ClientDBNameField = "client.db-name"
	//ClientAllSyncURLsField Field name of client.all-sync-urls
	ClientAllSyncURLsField = "client.all-sync-urls"
	//ClientStartTimeField Field name of client.start-time
	ClientStartTimeField = "client.start-time"
	//ClientBatchSizeField Field name of client.batch-size
	ClientBatchSizeField = "client.batch-size"
	//ClientWaitTimeField Field name of client.wait-time
	ClientWaitTimeField = "client.wait-time"
	//ClientSkipTimeField Field name of client.skip-time
	ClientSkipTimeField = "client.skip-time"

	//LoggerOutputField Field name of logger.output config
	LoggerOutputField = "logger.output"
	//LoggerFilePathField Field name of logger.file-path config
	LoggerFilePathField = "logger.file-path"
	//LoggerRotationTimeField Field name of logger.rotation-time config
	LoggerRotationTimeField = "logger.rotation-time"
	//LoggerMaxAgeField Field name of logger.rotation-time config
	LoggerMaxAgeField = "logger.max-age"
	//LoggerLevelField Field name of logger.level config
	LoggerLevelField = "logger.level"
)

//Config system configuration
type Config struct {
	Server *ServerConfig `mapstructure:"server"`
	Client *ClientConfig `mapstructure:"client"`
	Logger *LogConfig    `mapstructure:"logger"`
}

//ServerConfig server configuration
type ServerConfig struct {
	BindAddr    string `mapstructure:"bind-addr"`
	MongoDBURL  string `mapstructure:"mongodb-url"`
	DBName      string `mapstructure:"db-name"`
	MinerDBName string `mapstructure:"miner-db-name"`
	SNID        int    `mapstructure:"sn-id"`
	SkipTime    int    `mapstructure:"skip-time"`
}

//ClientConfig client configuration
type ClientConfig struct {
	MongoDBURL  string   `mapstructure:"mongodb-url"`
	DBName      string   `mapstructure:"db-name"`
	AllSyncURLs []string `mapstructure:"all-sync-urls"`
	StartTime   int32    `mapstructure:"start-time"`
	BatchSize   int      `mapstructure:"batch-size"`
	WaitTime    int      `mapstructure:"wait-time"`
	SkipTime    int      `mapstructure:"skip-time"`
}

//LogConfig system log configuration
type LogConfig struct {
	Output       string `mapstructure:"output"`
	FilePath     string `mapstructure:"file-path"`
	RotationTime int64  `mapstructure:"rotation-time"`
	MaxAge       int64  `mapstructure:"max-age"`
	Level        string `mapstructure:"level"`
}
