package routers

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/yottachain/yotta-sync-server/conf"
	"github.com/yottachain/yotta-sync-server/controllers"
)

//InitRouter 初始化路由
func InitRouter(cfg *conf.Config) (router *gin.Engine) {
	router = gin.Default()
	config := cors.DefaultConfig()
	config.AllowAllOrigins = true
	router.Use(cors.New(config))
	//t1 := controllers.DB{Mgo: db.InitMongoDB2()}
	dao, err := controllers.InitDao(cfg.GetRecieveInfo("url"), cfg)
	if err != nil {
		panic(err)
	}
	v1 := router.Group("/sync")
	{
		{
			v1.GET("/get_blocks", dao.GetBlocksByTimes)
			v1.GET("/get_shards", dao.GetShardsByBlockIDAndVNF)
			v1.GET("/get_receive", dao.ReceiveInfo)
			v1.GET("/createInitRecord", dao.CreateInitSyncRecord)
		}
	}

	return
}
