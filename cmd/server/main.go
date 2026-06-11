// 程序入口，负责三件事：
//   1. 依赖组装（手动注入，无 DI 框架）
//   2. 启动三个 goroutine（HTTP 服务 / Worker Pool / 信号监听）
//   3. 优雅关闭（先停 HTTP，再停 Worker Pool，有超时兜底）
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"reliable-webhook-platform/internal/api"
	"reliable-webhook-platform/internal/config"
	"reliable-webhook-platform/internal/db"
	"reliable-webhook-platform/internal/observability"
	"reliable-webhook-platform/internal/repository"
	"reliable-webhook-platform/internal/service"
	"reliable-webhook-platform/internal/worker"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// ==================== 第一阶段：配置和依赖初始化 ====================

	cfg, err := config.Load()
	if err != nil {
		logger.Error("load config failed", "err", err)
		os.Exit(1)
	}

	// 整个进程只有一个 *sql.DB，eventRepo 和 deliveryRepo 共享。
	// 所有 BeginTx 都从同一个连接池拿连接，FOR UPDATE 锁在同一个连接上，作用域明确。
	mysqlDB, err := db.NewMySQL(cfg.MySQLDSN)
	if err != nil {
		logger.Error("connect mysql failed", "err", err)
		os.Exit(1)
	}
	defer mysqlDB.Close()

	metrics := observability.NewMetrics(prometheus.DefaultRegisterer)

	// 两个 repo 共享同一个 *sql.DB
	eventRepo := repository.NewEventRepository(mysqlDB)
	deliveryRepo := repository.NewDeliveryRepository(mysqlDB)

	// 依赖链：repo → service → handler → router
	eventService := service.NewEventService(eventRepo, deliveryRepo, metrics)
	eventHandler := api.NewEventHandler(eventService)
	router := api.NewRouter(eventHandler, cfg.RequestTimeout)

	// 依赖链：repo + client → worker → pool
	webhookClient := worker.NewWebhookClient()
	deliveryWorker := worker.NewDeliveryWorker(
		deliveryRepo,
		webhookClient,
		logger,
		metrics,
		cfg.DeliveryTimeout,
	)

	// claimLease = DeliveryTimeout + PollInterval + 2s
	// 这个值决定了 running 状态的 delivery 多久后被判定为"worker 已死，可以被其他 worker 抢"。
	// 多加 poll_interval 和 2s 余量，防止正常投递还在跑就被误认领。
	deliveryPool := worker.NewDeliveryPool(
		deliveryRepo,
		deliveryWorker,
		logger,
		metrics,
		cfg.WorkerCount,
		cfg.QueueSize,
		cfg.PollInterval,
		cfg.DeliveryTimeout+cfg.PollInterval+2*time.Second,
	)

	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: router,
	}

	// ==================== 第二阶段：启动 goroutine ====================

	// rootCtx 监听 SIGTERM/SIGINT，用于触发优雅关闭
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// poolCtx 单独从 Background() 派生，不挂在 rootCtx 上。
	// 这意味着收到信号时 pool 不会立刻被 cancel ——
	// 必须先停 HTTP（不再接受新请求），再停 pool（让正在投递的 worker 跑完当前任务）。
	poolCtx, poolCancel := context.WithCancel(context.Background())
	poolDone := make(chan struct{})

	go func() {
		defer close(poolDone) // pool 完全退出后关闭，主 goroutine 用此确认"可以安全退出了"
		deliveryPool.Start(poolCtx)
	}()

	// serverErrCh 有缓冲（1），防止 ListenAndServe 出错时无缓冲 channel 永久阻塞导致 goroutine 泄露
	serverErrCh := make(chan error, 1)
	go func() {
		logger.Info("server starting",
			"addr", cfg.HTTPAddr,
			"request_timeout", cfg.RequestTimeout,
			"delivery_timeout", cfg.DeliveryTimeout,
			"shutdown_timeout", cfg.ShutdownTimeout,
			"worker_count", cfg.WorkerCount,
			"queue_size", cfg.QueueSize,
			"poll_interval", cfg.PollInterval,
		)

		// http.ErrServerClosed 是 server.Shutdown() 时产生的，属于正常关闭，不进错误通道
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
	}()

	// ==================== 第三阶段：等待退出信号 ====================

	select {
	case <-rootCtx.Done():
		// 收到 SIGTERM / SIGINT，正常退出
		logger.Info("shutdown signal received")
	case err := <-serverErrCh:
		// HTTP 启动就挂了（端口冲突等），直接 poolCancel + return
		// defer mysqlDB.Close() 仍会执行，因为不是 os.Exit
		logger.Error("server failed", "err", err)
		poolCancel()
		return
	}

	// ==================== 第四阶段：优雅关闭 ====================

	// 第一步：停 HTTP 服务器
	// shutdownCtx 有超时——等现有请求处理完，但不无限等
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown failed", "err", err)
	} else {
		logger.Info("http server shutdown complete")
	}

	// 第二步：停 Worker Pool
	// 此时 HTTP 已停，不会再有新的 /events 或 /events/:id/replay 请求进来
	// 但 worker 手上的投递任务还会跑完（poolCtx 还没 cancel）
	poolCancel()

	// 第三步：等 pool 退出
	// 如果 worker 卡在长时间 HTTP 调用中迟迟不退，超时兜底 — 不无限等
	select {
	case <-poolDone:
		logger.Info("delivery pool shutdown complete")
	case <-time.After(cfg.ShutdownTimeout):
		logger.Warn("delivery pool shutdown timed out")
	}

	logger.Info("service stopped")
}
