package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"reliable-webhook-platform/internal/repository"
	"reliable-webhook-platform/internal/service"
)

type EventHandler struct {
	eventService *service.EventService
}

func NewEventHandler(eventService *service.EventService) *EventHandler {
	return &EventHandler{eventService: eventService}
}

type CreateEventRequest struct {
	EventKey  string `json:"event_key" binding:"required"`
	EventType string `json:"event_type" binding:"required"`
	Payload   string `json:"payload" binding:"required"`
	TargetURL string `json:"target_url" binding:"required"`
}

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

	c.JSON(http.StatusOK, gin.H{
		"event_id": result.EventID,
		"created":  false,
		"message":  "event already exists, returned existing event",
	})
}

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

func (h *EventHandler) ReplayEvent(c *gin.Context) {
	idStr := c.Param("id")
	eventID, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid id",
		})
		return
	}

	deliveryID, err := h.eventService.ReplayEvent(c.Request.Context(), eventID)
	if err != nil {
		if errors.Is(err, repository.ErrActiveDeliveryExists) {
			c.JSON(http.StatusConflict, gin.H{
				"error": "active delivery already exists for this event",
			})
			return
		}
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