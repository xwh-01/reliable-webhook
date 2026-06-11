// 业务逻辑层，负责：
//   1. 参数校验（空值检查）
//   2. 幂等处理（event_key 冲突 → 回查已有 id 返回）
//   3. 透传调用 repository（GetEventDetail / ReplayEvent / ListDeliveriesByStatus）
package service

import (
	"context"
	"errors"
	"strings"

	"reliable-webhook-platform/internal/observability"
	"reliable-webhook-platform/internal/model"
	"reliable-webhook-platform/internal/repository"
)

type EventService struct {
	eventRepo    *repository.EventRepository
	deliveryRepo *repository.DeliveryRepository
	metrics      *observability.Metrics
}

func NewEventService(
	eventRepo *repository.EventRepository,
	deliveryRepo *repository.DeliveryRepository,
	metrics *observability.Metrics,
) *EventService {
	return &EventService{
		eventRepo:    eventRepo,
		deliveryRepo: deliveryRepo,
		metrics:      metrics,
	}
}

type CreateEventInput struct{
	EventKey string
	EventType string
	Payload string
	TargetURL string
}

type CreateEventResult struct {
	EventID int64
	Created bool // true = 新创建，false = 重复提交返回已有 id
}


func (s *EventService) CreateEvent(ctx context.Context,input CreateEventInput)(*CreateEventResult,error){
	// 空值校验：四个必填字段不能全为空或空白
    if strings.TrimSpace(input.EventKey) == "" ||
		strings.TrimSpace(input.EventType) == "" ||
		strings.TrimSpace(input.Payload) == "" ||
		strings.TrimSpace(input.TargetURL) == "" {
		if s.metrics != nil {
			s.metrics.EventsReceivedTotal.WithLabelValues("invalid").Inc()
		}
		return nil, errors.New("missing required fields")
	}

	// 尝试创建事件 + 投递（在同一事务中）
	eventID, err := s.eventRepo.CreateEventWithDelivery(ctx, repository.CreateEventParams{
		EventKey:  input.EventKey,
		EventType: input.EventType,
		Payload:   input.Payload,
		TargetURL: input.TargetURL,
	})
	if err != nil {
		// event_key 重复 → 幂等处理：回查已有 id 返回
		// 不报错，业务方收到的响应和首次提交一致（event_id 相同，created=false）
		if errors.Is(err, repository.ErrEventKeyConflict) {
			existingID, getErr := s.eventRepo.GetEventIDByKey(ctx, input.EventKey)
			if getErr != nil {
				if s.metrics != nil {
					s.metrics.EventsReceivedTotal.WithLabelValues("error").Inc()
				}
				return nil, getErr
			}
			if s.metrics != nil {
				s.metrics.EventsReceivedTotal.WithLabelValues("duplicate").Inc()
			}
			return &CreateEventResult{
				EventID: existingID,
				Created: false,
			}, nil
		}
		return nil, err
	}

	if s.metrics != nil {
		s.metrics.EventsReceivedTotal.WithLabelValues("created").Inc()
	}

	return &CreateEventResult{
		EventID: eventID,
		Created: true,
	}, nil
}

func (s *EventService) GetEventDetail(ctx context.Context, id int64) (*model.EventDetail, error) {
	return s.eventRepo.GetEventDetailByID(ctx, id)
}

func (s *EventService) ReplayEvent(ctx context.Context, eventID int64) (int64, error) {
	return s.deliveryRepo.CreateReplayDelivery(ctx, eventID)
}

func (s *EventService) ListDeliveriesByStatus(ctx context.Context, status string) ([]model.AdminDeliveryInfo, error) {
	return s.deliveryRepo.ListByStatus(ctx, status)
}
