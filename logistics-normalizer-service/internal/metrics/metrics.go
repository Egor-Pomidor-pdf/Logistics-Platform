package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Collector stores service metrics.
type Collector struct {
	rawEvents                *prometheus.CounterVec
	normalizedEvents         *prometheus.CounterVec
	invalidEvents            *prometheus.CounterVec
	deduplicatedEvents       prometheus.Counter
	statusNormalizationError *prometheus.CounterVec
	normalizationErrors      prometheus.Counter
	kafkaHandlerDuration     prometheus.Histogram
	kafkaConsumerLag         prometheus.Gauge
}

// New creates and registers all metrics.
func New() *Collector {
	return &Collector{
		rawEvents: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "logistics_raw_events_total",
			Help: "Count of consumed raw logistics events.",
		}, []string{"source_system"}),
		normalizedEvents: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "logistics_normalized_events_total",
			Help: "Count of normalized logistics events published to Kafka.",
		}, []string{"source_system", "status_code"}),
		invalidEvents: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "logistics_events_invalid_total",
			Help: "Count of invalid events routed to invalid topic.",
		}, []string{"reason"}),
		deduplicatedEvents: promauto.NewCounter(prometheus.CounterOpts{
			Name: "logistics_events_deduplicated_total",
			Help: "Count of duplicate events filtered by event_id.",
		}),
		statusNormalizationError: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "status_normalization_errors_total",
			Help: "Count of mapping errors by source and external status.",
		}, []string{"source_system", "external_status"}),
		normalizationErrors: promauto.NewCounter(prometheus.CounterOpts{
			Name: "normalization_errors_total",
			Help: "Count of total normalization processing errors.",
		}),
		kafkaHandlerDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "normalizer_kafka_handler_duration_seconds",
			Help:    "Duration of raw Kafka message processing.",
			Buckets: prometheus.DefBuckets,
		}),
		kafkaConsumerLag: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "kafka_consumer_lag",
			Help: "Approximate Kafka consumer lag in messages.",
		}),
	}
}

// Handler returns Prometheus HTTP handler.
func (c *Collector) Handler() http.Handler {
	return promhttp.Handler()
}

func (c *Collector) IncRawEvents(sourceSystem string) {
	c.rawEvents.WithLabelValues(sourceSystem).Inc()
}

func (c *Collector) IncNormalizedEvents(sourceSystem, statusCode string) {
	c.normalizedEvents.WithLabelValues(sourceSystem, statusCode).Inc()
}

func (c *Collector) IncInvalidEvents(reason string) {
	c.invalidEvents.WithLabelValues(reason).Inc()
}

func (c *Collector) IncDeduplicatedEvents() {
	c.deduplicatedEvents.Inc()
}

func (c *Collector) IncStatusNormalizationError(sourceSystem, externalStatus string) {
	c.statusNormalizationError.WithLabelValues(sourceSystem, externalStatus).Inc()
}

func (c *Collector) IncNormalizationError() {
	c.normalizationErrors.Inc()
}

func (c *Collector) ObserveKafkaHandlerDuration(duration time.Duration) {
	c.kafkaHandlerDuration.Observe(duration.Seconds())
}

func (c *Collector) SetKafkaConsumerLag(lag float64) {
	if lag < 0 {
		lag = 0
	}
	c.kafkaConsumerLag.Set(lag)
}
