// 单次投递的逻辑处理（被 DeliveryPool 的 worker goroutine 调用）
//
// 一次投递的完整流程：
//   1. HTTP POST 到 target_url
//   2. 成功（2xx）→ RecordAttempt(succeeded) + MarkSucceeded → 结束
//   3. 失败→ 判断：
//      a. 不可重试错误（非 429 的 4xx）→ RecordAttempt(failed) + MarkDead("dead_non_retryable")
//      b. 达到最大重试次数 → RecordAttempt(failed) + MarkDead("dead_max_attempts")
//      c. 可重试错误（超时/429/5xx）→ RecordAttempt(failed) + ScheduleRetry（状态回 pending）
//
// 注意：context 管理
//   execCtx 从 loopCtx 派生，有 deliveryTimeout 超时。如果超时，HTTP 调用被中断。
//   updateCtx 从 Background() 派生，有 2s 超时。即使 loopCtx 被 cancel（pool 正在关闭），
//   状态更新仍会尝试写入数据库，避免 delivery 卡在 running 状态无回写。
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

// Process 处理一次投递任务
// loopCtx 来自 pool 的 context，pool 关闭时会 cancel
// 但状态更新用的是 Background() 派生的 context，保证退出时也能写库
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

	// 超时控制：HTTP 调用有 deliveryTimeout 上限
	execCtx, cancel := context.WithTimeout(loopCtx, w.deliveryTimeout)
	result := w.client.Send(execCtx, delivery.TargetURL, delivery.Payload)
	cancel()

	attemptNo := delivery.AttemptCount

	// 状态更新用的 context：
	// 从 Background() 派生，有独立 2s 超时。
	// 不能用 loopCtx — loopCtx 可能已取消（pool 关闭过程中），
	// 但我们仍需要把 delivery 状态写回数据库，避免卡在 running。
	updateCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	statusCodeLabel := "none"
	if result.StatusCode != 0 {
		statusCodeLabel = strconv.Itoa(result.StatusCode)
	}

	// ========== 失败分支 ==========
	if result.Err != nil {
		errMsg := result.Err.Error()

		// 每次失败都记录 attempt，用于事后排查
		_ = w.repo.RecordAttempt(updateCtx, delivery.ID, attemptNo, "failed", &errMsg, intPtrOrNil(result.StatusCode))

		if w.metrics != nil {
			w.metrics.DeliveryAttemptsTotal.WithLabelValues("failed", statusCodeLabel).Inc()
		}

		// 情况 1：不可重试的错误（400, 401, 403, 404, 405 等非 429 的 4xx）
		// → 直接 dead，不再重试
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

		// 情况 2：可重试但已达最大次数
		// → dead，但有完整 attempt 记录可追溯
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

		// 情况 3：可重试且还有次数
		// → schedule retry，状态回退到 pending，下次重试时间 = now + backoff(attemptNo)
		//    第一次重试：5s 后
		//    第二次重试：15s 后
		//    第三次及之后：30s 后
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

	// ========== 成功分支 ==========
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

// backoff 退避时间：尝试次数越多，间隔越长
//   1 → 5s
//   2 → 15s
//   3+ → 30s（封顶）
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

// intPtrOrNil 将 int 转成 *int，0 返回 nil（表示无状态码）
func intPtrOrNil(v int) *int {
	if v == 0 {
		return nil
	}
	return &v
}
