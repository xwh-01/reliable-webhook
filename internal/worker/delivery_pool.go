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
	workerCount  int
	queueSize    int
	pollInterval time.Duration
	claimLease   time.Duration
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

func (p *DeliveryPool) Start(ctx context.Context) {
	jobs := make(chan repository.ClaimedDelivery, p.queueSize)

	var wg sync.WaitGroup

	// 启动固定数量 worker
	for i := 0; i < p.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			p.logger.Info("delivery pool worker started", "worker_id", workerID)

			for {
				select {
				case <-ctx.Done():
					p.logger.Info("delivery pool worker stopped", "worker_id", workerID)
					return
				case job, ok := <-jobs:
					if !ok {
						p.logger.Info("delivery pool worker channel closed", "worker_id", workerID)
						return
					}

					if p.metrics != nil {
						p.metrics.DeliveryQueueDepth.Dec()
					}

					p.worker.Process(ctx, job)
				}
			}
		}(i + 1)
	}

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
			if len(jobs) >= cap(jobs) {
				p.logger.Warn("delivery queue is full, skipping claim",
					"queue_depth", len(jobs),
					"queue_size", cap(jobs),
				)
				continue
			}

			claimCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			claimed, err := p.repo.ClaimOneReadyPending(claimCtx, time.Now().Add(p.claimLease))
			cancel()
			if err != nil {
				p.logger.Error("claim delivery failed", "err", err)
				continue
			}
			if claimed == nil {
				continue
			}

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
				break dispatchLoop
			}
		}
	}

	close(jobs)
	wg.Wait()
	p.logger.Info("delivery pool stopped")
}
