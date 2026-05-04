package observability

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	EventsReceivedTotal     *prometheus.CounterVec
	DeliveryAttemptsTotal   *prometheus.CounterVec
	DeliveryFinalStateTotal *prometheus.CounterVec
	DeliveryDurationSeconds *prometheus.HistogramVec
	DeliveriesInFlight      prometheus.Gauge
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