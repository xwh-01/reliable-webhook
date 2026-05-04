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

	cfg, err := config.Load()
	if err != nil {
		logger.Error("load config failed", "err", err)
		os.Exit(1)
	}

	mysqlDB, err := db.NewMySQL(cfg.MySQLDSN)
	if err != nil {
		logger.Error("connect mysql failed", "err", err)
		os.Exit(1)
	}
	defer mysqlDB.Close()

	metrics := observability.NewMetrics(prometheus.DefaultRegisterer)

	eventRepo := repository.NewEventRepository(mysqlDB)
	deliveryRepo := repository.NewDeliveryRepository(mysqlDB)

	eventService := service.NewEventService(eventRepo, deliveryRepo, metrics)
	eventHandler := api.NewEventHandler(eventService)

	router := api.NewRouter(eventHandler, cfg.RequestTimeout)

	webhookClient := worker.NewWebhookClient()
	deliveryWorker := worker.NewDeliveryWorker(
		deliveryRepo,
		webhookClient,
		logger,
		metrics,
		cfg.DeliveryTimeout,
	)

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

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	poolCtx, poolCancel := context.WithCancel(context.Background())
	poolDone := make(chan struct{})

	go func() {
		defer close(poolDone)
		deliveryPool.Start(poolCtx)
	}()

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

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
	}()

	select {
	case <-rootCtx.Done():
		logger.Info("shutdown signal received")
	case err := <-serverErrCh:
		logger.Error("server failed", "err", err)
		poolCancel()
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown failed", "err", err)
	} else {
		logger.Info("http server shutdown complete")
	}

	poolCancel()

	select {
	case <-poolDone:
		logger.Info("delivery pool shutdown complete")
	case <-time.After(cfg.ShutdownTimeout):
		logger.Warn("delivery pool shutdown timed out")
	}

	logger.Info("service stopped")
}
