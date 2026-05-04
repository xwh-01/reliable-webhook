package worker

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"reliable-webhook-platform/internal/observability"
	"reliable-webhook-platform/internal/repository"
)

type DeliveryWorker struct {
	repo            *repository.DeliveryRepository
	client          *WebhookClient
	logger          *slog.Logger
	metrics         *observability.Metrics
	deliveryTimeout time.Duration
}

func NewDeliveryWorker(
	repo *repository.DeliveryRepository,
	client *WebhookClient,
	logger *slog.Logger,
	metrics *observability.Metrics,
	deliveryTimeout time.Duration,
) *DeliveryWorker {
	return &DeliveryWorker{
		repo:            repo,
		client:          client,
		logger:          logger,
		metrics:         metrics,
		deliveryTimeout: deliveryTimeout,
	}
}

func (w *DeliveryWorker) Process(loopCtx context.Context, delivery repository.ClaimedDelivery) {
	start := time.Now()
	if w.metrics != nil {
		w.metrics.DeliveriesInFlight.Inc()
		defer w.metrics.DeliveriesInFlight.Dec()
	}

	w.logger.Info("delivery running",
		"delivery_id", delivery.ID,
		"event_id", delivery.EventID,
		"attempt_count", delivery.AttemptCount,
		"max_attempts", delivery.MaxAttempts,
	)

	execCtx, cancel := context.WithTimeout(loopCtx, w.deliveryTimeout)
	result := w.client.Send(execCtx, delivery.TargetURL, delivery.Payload)
	cancel()

	attemptNo := delivery.AttemptCount

	updateCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	statusCodeLabel := "none"
	if result.StatusCode != 0 {
		statusCodeLabel = strconv.Itoa(result.StatusCode)
	}

	if result.Err != nil {
		errMsg := result.Err.Error()

		_ = w.repo.RecordAttempt(updateCtx, delivery.ID, attemptNo, "failed", &errMsg, intPtrOrNil(result.StatusCode))

		if w.metrics != nil {
			w.metrics.DeliveryAttemptsTotal.WithLabelValues("failed", statusCodeLabel).Inc()
		}

		if !result.Retryable {
			if markErr := w.repo.MarkDead(updateCtx, delivery.ID, errMsg); markErr != nil {
				w.logger.Error("mark dead failed",
					"delivery_id", delivery.ID,
					"err", markErr,
				)
				return
			}

			if w.metrics != nil {
				w.metrics.DeliveryFinalStateTotal.WithLabelValues("dead_non_retryable").Inc()
				w.metrics.DeliveryDurationSeconds.WithLabelValues("dead_non_retryable").Observe(time.Since(start).Seconds())
			}

			w.logger.Error("delivery dead (non-retryable)",
				"delivery_id", delivery.ID,
				"event_id", delivery.EventID,
				"attempt_no", attemptNo,
				"status_code", result.StatusCode,
				"err", result.Err,
			)
			return
		}

		if attemptNo >= delivery.MaxAttempts {
			if markErr := w.repo.MarkDead(updateCtx, delivery.ID, errMsg); markErr != nil {
				w.logger.Error("mark dead failed",
					"delivery_id", delivery.ID,
					"err", markErr,
				)
				return
			}

			if w.metrics != nil {
				w.metrics.DeliveryFinalStateTotal.WithLabelValues("dead_max_attempts").Inc()
				w.metrics.DeliveryDurationSeconds.WithLabelValues("dead_max_attempts").Observe(time.Since(start).Seconds())
			}

			w.logger.Error("delivery dead (max attempts reached)",
				"delivery_id", delivery.ID,
				"event_id", delivery.EventID,
				"attempt_no", attemptNo,
				"status_code", result.StatusCode,
				"err", result.Err,
			)
			return
		}

		nextRetryAt := time.Now().Add(backoff(attemptNo))
		if retryErr := w.repo.ScheduleRetry(updateCtx, delivery.ID, errMsg, nextRetryAt); retryErr != nil {
			w.logger.Error("schedule retry failed",
				"delivery_id", delivery.ID,
				"err", retryErr,
			)
			return
		}

		if w.metrics != nil {
			w.metrics.DeliveryFinalStateTotal.WithLabelValues("retry_scheduled").Inc()
			w.metrics.DeliveryDurationSeconds.WithLabelValues("retry_scheduled").Observe(time.Since(start).Seconds())
		}

		w.logger.Warn("delivery scheduled for retry",
			"delivery_id", delivery.ID,
			"event_id", delivery.EventID,
			"attempt_no", attemptNo,
			"status_code", result.StatusCode,
			"next_retry_at", nextRetryAt,
			"err", result.Err,
		)
		return
	}

	_ = w.repo.RecordAttempt(updateCtx, delivery.ID, attemptNo, "succeeded", nil, intPtrOrNil(result.StatusCode))

	if w.metrics != nil {
		w.metrics.DeliveryAttemptsTotal.WithLabelValues("succeeded", statusCodeLabel).Inc()
	}

	if err := w.repo.MarkSucceeded(updateCtx, delivery.ID); err != nil {
		w.logger.Error("mark success failed",
			"delivery_id", delivery.ID,
			"err", err,
		)
		return
	}

	if w.metrics != nil {
		w.metrics.DeliveryFinalStateTotal.WithLabelValues("succeeded").Inc()
		w.metrics.DeliveryDurationSeconds.WithLabelValues("succeeded").Observe(time.Since(start).Seconds())
	}

	w.logger.Info("delivery succeeded",
		"delivery_id", delivery.ID,
		"event_id", delivery.EventID,
		"attempt_no", attemptNo,
		"status_code", result.StatusCode,
	)
}

func backoff(attemptNo int) time.Duration {
	switch attemptNo {
	case 1:
		return 5 * time.Second
	case 2:
		return 15 * time.Second
	default:
		return 30 * time.Second
	}
}

func intPtrOrNil(v int) *int {
	if v == 0 {
		return nil
	}
	return &v
}
