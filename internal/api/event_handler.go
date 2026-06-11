// HTTP Handler 层，负责：
//   1. 解析请求参数、校验必填字段
//   2. 调用 Service 层
//   3. 根据 Service 返回结果映射 HTTP 状态码和响应体
//
// 不做业务逻辑 — 业务全在 service 层
package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"reliable-webhook-platform/internal/repository"
	"reliable-webhook-platform/internal/service"
)

// EventHandler 持有 EventService，所有方法通过它调用业务逻辑
type EventHandler struct {
	eventService *service.EventService
}

func NewEventHandler(eventService *service.EventService) *EventHandler {
	return &EventHandler{eventService: eventService}
}

// CreateEventRequest 创建事件的请求体
type CreateEventRequest struct {
	EventKey  string `json:"event_key" binding:"required"`
	EventType string `json:"event_type" binding:"required"`
	Payload   string `json:"payload" binding:"required"`
	TargetURL string `json:"target_url" binding:"required"`
}

// CreateEvent 提交事件
// 幂等：相同 event_key 重复提交不会重复创建，返回已有 event_id + created=false
func (h *EventHandler) CreateEvent(c *gin.Context) {
	var req CreateEventRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request body",
		})
		return
	}

	result, err := h.eventService.CreateEvent(c.Request.Context(), service.CreateEventInput{
		EventKey:  req.EventKey,
		EventType: req.EventType,
		Payload:   req.Payload,
		TargetURL: req.TargetURL,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	if result.Created {
		c.JSON(http.StatusCreated, gin.H{
			"event_id": result.EventID,
			"created":  true,
		})
		return
	}

	// 重复提交：幂等返回已有 event_id
	c.JSON(http.StatusOK, gin.H{
		"event_id": result.EventID,
		"created":  false,
		"message":  "event already exists, returned existing event",
	})
}

// GetEvent 查询事件详情（Event 信息 + 最新一条 Delivery 信息）
// 返回的 delivery 是 id DESC 最新的一条，可能是 dead / succeeded / replay 后的新行
func (h *EventHandler) GetEvent(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid id",
		})
		return
	}

	detail, err := h.eventService.GetEventDetail(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "query event failed",
		})
		return
	}
	if detail == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "event not found",
		})
		return
	}

	// 扁平分开展示，event 和 delivery 各一个对象
	c.JSON(http.StatusOK, gin.H{
		"event": gin.H{
			"id":         detail.Event.ID,
			"event_key":  detail.Event.EventKey,
			"event_type": detail.Event.EventType,
			"payload":    detail.Event.Payload,
			"status":     detail.Event.Status,
			"created_at": detail.Event.CreatedAt,
		},
		"delivery": gin.H{
			"id":                    detail.Delivery.ID,
			"event_id":              detail.Delivery.EventID,
			"target_url":            detail.Delivery.TargetURL,
			"status":                detail.Delivery.Status,
			"last_error":            detail.Delivery.LastError,
			"attempt_count":         detail.Delivery.AttemptCount,
			"max_attempts":          detail.Delivery.MaxAttempts,
			"replay_of_delivery_id": detail.Delivery.ReplayOfDeliveryID,
			"created_at":            detail.Delivery.CreatedAt,
			"updated_at":            detail.Delivery.UpdatedAt,
		},
	})
}

// ReplayEvent 人工重放
// 为指定 event 创建一条全新的 delivery，status='pending'。
// 如果有活跃中的 delivery（pending/running），返回 409 拒绝。
// 如果该 event 没有历史 delivery，返回 404。
func (h *EventHandler) ReplayEvent(c *gin.Context) {
	idStr := c.Param("id")
	// 这里的 id 是 events.id（不是 deliveries.id）
	eventID, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid id",
		})
		return
	}

	deliveryID, err := h.eventService.ReplayEvent(c.Request.Context(), eventID)
	if err != nil {
		// 活跃 delivery 存在 — 同一条事件只能有一个投递在执行
		if errors.Is(err, repository.ErrActiveDeliveryExists) {
			c.JSON(http.StatusConflict, gin.H{
				"error": "active delivery already exists for this event",
			})
			return
		}
		// 该事件没有任何 delivery 记录
		if errors.Is(err, repository.ErrDeliveryNotFound) {
			c.JSON(http.StatusNotFound, gin.H{
				"error": "no delivery found for this event",
			})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "replay event failed",
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"delivery_id": deliveryID,
		"status":      "pending",
	})
}

// ListDeliveries 按状态列出投递记录（运营后台用）
// 默认查询 status=dead 的记录
func (h *EventHandler) ListDeliveries(c *gin.Context) {
	status := c.DefaultQuery("status", "dead") // 默认查 dead，运营最关心这个

	list, err := h.eventService.ListDeliveriesByStatus(c.Request.Context(), status)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "query deliveries failed",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"deliveries": list,
	})
}
