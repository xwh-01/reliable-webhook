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
	r.Use(RequestTimeout(requestTimeout))

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.POST("/events", eventHandler.CreateEvent)
	r.GET("/events/:id", eventHandler.GetEvent)
    r.POST("/events/:id/replay", eventHandler.ReplayEvent)

	r.POST("/mock-downstream", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "mock downstream received",
		})
	})

	r.POST("/mock-downstream-slow", func(c *gin.Context) {
		time.Sleep(8 * time.Second)
		c.JSON(http.StatusOK, gin.H{
			"message": "mock slow downstream received",
		})
	})

	r.POST("/mock-downstream-500", func(c *gin.Context) {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "mock downstream internal error",
		})
	})

	r.POST("/mock-downstream-400", func(c *gin.Context) {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "mock downstream bad request",
		})
	})

	return r
}