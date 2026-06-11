// Prometheus 指标定义
//
// 6 项指标覆盖创建、投递、最终状态三个阶段的观测：
//   Counter  — 只增不减（累计投递次数、事件数）
//   Gauge    — 可增可减（当前正在投递的数量、队列深度）
//   Histogram— 分布统计（投递耗时分布）
package observability

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	// 事件接收统计，按 result 分类：created / duplicate / invalid / error
	EventsReceivedTotal     *prometheus.CounterVec

	// 投递尝试统计，按 outcome（succeeded/failed）+ status_code 分类
	DeliveryAttemptsTotal   *prometheus.CounterVec

	// 投递最终状态统计：
	//   state 取值：succeeded / dead_non_retryable / dead_max_attempts / retry_scheduled
	DeliveryFinalStateTotal *prometheus.CounterVec

	// 单次投递耗时分布，按 outcome 分类
	DeliveryDurationSeconds *prometheus.HistogramVec

	// 当前正在执行的投递数量
	DeliveriesInFlight      prometheus.Gauge

	// 内存队列中等待处理的投递数量
	DeliveryQueueDepth      prometheus.Gauge
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		EventsReceivedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "webhook_events_received_total",
				Help: "Total number of event receive attempts",
			},
			[]string{"result"},
		),
		DeliveryAttemptsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "webhook_delivery_attempts_total",
				Help: "Total number of delivery attempts",
			},
			[]string{"outcome", "status_code"},
		),
		DeliveryFinalStateTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "webhook_delivery_final_state_total",
				Help: "Total number of delivery final state transitions",
			},
			[]string{"state"},
		),
		DeliveryDurationSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "webhook_delivery_duration_seconds",
				Help:    "Duration of one delivery execution attempt",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"outcome"},
		),
		DeliveriesInFlight: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "webhook_deliveries_in_flight",
				Help: "Number of deliveries currently being executed",
			},
		),
		DeliveryQueueDepth: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "webhook_delivery_queue_depth",
				Help: "Number of claimed deliveries waiting in the in-memory queue",
			},
		),
	}

	reg.MustRegister(
		m.EventsReceivedTotal,
		m.DeliveryAttemptsTotal,
		m.DeliveryFinalStateTotal,
		m.DeliveryDurationSeconds,
		m.DeliveriesInFlight,
		m.DeliveryQueueDepth,
	)

	return m
}
