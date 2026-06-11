// Worker Pool：生产者-消费者模型
//
// 结构：
//   Dispatcher（主 goroutine，定时轮询数据库）→ jobs channel → Worker goroutine（消费执行）
//
// 流程：
//   1. Dispatcher 每 poll_interval 秒执行 ClaimOneReadyPending
//   2. 认领到的任务扔进有缓冲的 jobs channel
//   3. N 个 worker goroutine 从 channel 取任务，调用 DeliveryWorker.Process
//   4. context 取消 → dispatcher 停轮询，channel 排空，worker 退出
//
// 为什么用有缓冲 channel 而不是直接调：
//   - 数据库认领和 HTTP 投递速度不匹配时，channel 做削峰缓冲
//   - dispatcher 不被慢投递阻塞，能继续认领新任务
//   - channel 满了跳过本次认领（不是阻塞等），防止 dispatcher 卡死
package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"reliable-webhook-platform/internal/observability"
	"reliable-webhook-platform/internal/repository"
)

type DeliveryPool struct {
	repo         *repository.DeliveryRepository
	worker       *DeliveryWorker
	logger       *slog.Logger
	metrics      *observability.Metrics
	workerCount  int           // worker goroutine 数量
	queueSize    int           // jobs channel 缓冲大小
	pollInterval time.Duration // dispatcher 轮询间隔
	claimLease   time.Duration // 认领后的租约时长（locked_until = now + claimLease）
}

func NewDeliveryPool(
	repo *repository.DeliveryRepository,
	worker *DeliveryWorker,
	logger *slog.Logger,
	metrics *observability.Metrics,
	workerCount int,
	queueSize int,
	pollInterval time.Duration,
	claimLease time.Duration,
) *DeliveryPool {
	return &DeliveryPool{
		repo:         repo,
		worker:       worker,
		logger:       logger,
		metrics:      metrics,
		workerCount:  workerCount,
		queueSize:    queueSize,
		pollInterval: pollInterval,
		claimLease:   claimLease,
	}
}

// Start 启动 dispatcher + worker goroutine，阻塞直到 ctx 取消
func (p *DeliveryPool) Start(ctx context.Context) {
	jobs := make(chan repository.ClaimedDelivery, p.queueSize)

	var wg sync.WaitGroup

	// ========== 启动固定数量 worker ==========
	for i := 0; i < p.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			p.logger.Info("delivery pool worker started", "worker_id", workerID)

			for {
				select {
				case <-ctx.Done():
					// pool 关闭：退出循环
					p.logger.Info("delivery pool worker stopped", "worker_id", workerID)
					return
				case job, ok := <-jobs:
					if !ok {
						// channel 已关闭（dispatcher 退出后 close(jobs)），退出
						p.logger.Info("delivery pool worker channel closed", "worker_id", workerID)
						return
					}

					if p.metrics != nil {
						p.metrics.DeliveryQueueDepth.Dec()
					}

					p.worker.Process(ctx, job) // 同步调用，一个 worker 一次处理一条
				}
			}
		}(i + 1)
	}

	// ========== Dispatcher：定时轮询数据库，认领任务，投入 channel ==========
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	p.logger.Info("delivery dispatcher started",
		"worker_count", p.workerCount,
		"queue_size", p.queueSize,
		"poll_interval", p.pollInterval,
	)

dispatchLoop:
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("delivery dispatcher stopped")
			break dispatchLoop
		case <-ticker.C:
			// 队列满了跳过本次认领，不阻塞 dispatcher
			// 如果阻塞等 channel 有空间，ticker 积压，认领可能过期
			if len(jobs) >= cap(jobs) {
				p.logger.Warn("delivery queue is full, skipping claim",
					"queue_depth", len(jobs),
					"queue_size", cap(jobs),
				)
				continue
			}

			// 认领一个可投递任务
			// claimCtx 有独立 2s 超时，防止数据库慢查询拖死 timer
			claimCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			claimed, err := p.repo.ClaimOneReadyPending(claimCtx, time.Now().Add(p.claimLease))
			cancel()
			if err != nil {
				p.logger.Error("claim delivery failed", "err", err)
				continue
			}
			if claimed == nil {
				// 没有可投递任务，继续下次轮询
				continue
			}

			// 投入 channel 给 worker 消费
			select {
			case jobs <- *claimed:
				if p.metrics != nil {
					p.metrics.DeliveryQueueDepth.Inc()
				}
				p.logger.Info("delivery enqueued",
					"delivery_id", claimed.ID,
					"event_id", claimed.EventID,
					"queue_depth", len(jobs),
				)
			case <-ctx.Done():
				break dispatchLoop // 投递过程中收到关闭信号
			}
		}
	}

	// dispatcher 退出 → 关 channel → worker 排空剩余任务后退出
	close(jobs)
	wg.Wait()
	p.logger.Info("delivery pool stopped")
}
