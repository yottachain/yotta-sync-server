package routers

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/yottachain/yotta-sync-server/controllers"
	"github.com/yottachain/yotta-sync-server/db"
)

//InitRouter 初始化路由
func InitRouter() (router *gin.Engine) {
	router = gin.Default()
	config := cors.DefaultConfig()
	config.AllowAllOrigins = true
	router.Use(cors.New(config))
	t1 := controllers.DB{Mgo: db.InitMongoDB2()}
	v1 := router.Group("/sync")
	{
		{
			v1.GET("/get_blocks", t1.GetBlocksByTimes)
			v1.GET("/get_shards", t1.GetShardsByBlockIDAndVNF)
			v1.GET("/get_receive", t1.ReceiveInfo)
			v1.GET("/createInitRecord", t1.CreateInitSyncRecord)
		}
	}

	return
}
