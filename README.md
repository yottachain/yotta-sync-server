# yotta-sync-server
SN数据同步服务，服务端与客户端均整合至该服务中，使用不同子命令启动
## 1. 部署与配置：
在项目的main目录下编译：
```
$ go build -o yotta-sync-server
```
配置文件为`yotta-sync-server.yaml`（项目的main目录下有示例），默认可以放在`home`目录或重建程序同目录下，各配置项说明如下：
```
#服务端配置
server:
  #服务端监听地址
  bind-addr: ":8080"
  #服务端的源数据库URL
  mongodb-url: "mongodb://127.0.0.1:27017/?connect=direct"
  #源数据库名
  db-name: "metabase"
  #所属SN编号
  sn-id: 0
  #最近几分钟内的数据不进行同步，用于调用GetStoredShards接口时避免一致性问题，单位为秒
  skip-time: 300
#客户端配置
client:
  #客户端的同步数据库URL
  mongodb-url: "mongodb://127.0.0.1:27017/?retryWrites=true"
  #同步数据库名
  db-name: "metabase"
  #要连接的服务端地址，按SN编号顺序依次配置
  all-sync-urls:
  - "http://127.0.0.1:8051"
  - "http://127.0.0.1:8052"
  - "http://127.0.0.1:8053"
  #从该时间点开始进行同步，格式为UNIX时间戳
  start-time: 0
  #同步时每次拉取的分块数量
  batch-size: 10000
  #当没有新数据可同步时进程等待的时间，单位为秒
  wait-time: 30
  #最近几分钟内的数据不进行同步，用于避免一致性问题，单位为秒
  skip-time: 300
#日志配置
logger:
  #日志输出类型：stdout为输出到标准输出流，file为输出到文件，默认为stdout，此时只有level属性起作用，其他属性会被忽略
  output: "file"
  #日志路径，默认值为./rebuilder.log，仅在output=file时有效
  file-path: "./sync.log"
  #日志拆分间隔时间，默认为24（小时），仅在output=file时有效
  rotation-time: 24
  #日志最大保留时间，默认为240（10天），仅在output=file时有效
  max-age: 240
  #日志输出等级，默认为Info
  level: "Info"
```

# 2. 启动服务
## 2.1 服务端启动
服务端配置设置完毕后执行以下命令启动：
```
$ nohup ./yotta-sync-server server &
```

## 2.2 客户端启动
客户端配置设置完毕后执行以下命令启动：
```
$ nohup ./yotta-sync-server client &
```

# 3. 数据库配置
需要为同步库metabase的shards表建立索引
```
mongoshell> use metabase
mongoshell> db.shards.createIndex({nodeId:1, _id:1})
```