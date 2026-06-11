// HTTP 路由层，负责：
//   1. 注册业务端点（events 增删查 replay）
//   2. 注册可观测端点（/healthz /metrics）
//   3. 注册本地测试用的 mock 下游端点
//   4. 挂载全局中间件（超时、panic 恢复）
package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewRouter(eventHandler *EventHandler, requestTimeout time.Duration) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(RequestTimeout(requestTimeout)) // 每个 HTTP 请求有独立超时

	// 可观测端点
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	r.GET("/metrics", gin.WrapH(promhttp.Handler())) // Prometheus 抓取指标

	// 业务端点
	r.POST("/events", eventHandler.CreateEvent)        // 提交事件
	r.GET("/events/:id", eventHandler.GetEvent)        // 查询事件+投递状态
	r.POST("/events/:id/replay", eventHandler.ReplayEvent) // 人工重放

	// 管理后台
	r.GET("/admin/deliveries", eventHandler.ListDeliveries) // 按状态列出投递（运营用）
	r.StaticFile("/admin", "web/admin.html")               // 简易管理页面

	// ======= 以下是本地测试用的 mock 下游端点，不参与业务 =======

	// 正常返回 200
	r.POST("/mock-downstream", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "mock downstream received",
		})
	})

	// 睡眠 8 秒后返回 200，用于模拟下游超时
	r.POST("/mock-downstream-slow", func(c *gin.Context) {
		time.Sleep(8 * time.Second)
		c.JSON(http.StatusOK, gin.H{
			"message": "mock slow downstream received",
		})
	})

	// 返回 500，模拟下游服务错误（可重试）
	r.POST("/mock-downstream-500", func(c *gin.Context) {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "mock downstream internal error",
		})
	})

	// 返回 400，模拟下游参数错误（不可重试）
	r.POST("/mock-downstream-400", func(c *gin.Context) {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "mock downstream bad request",
		})
	})

	return r
}
