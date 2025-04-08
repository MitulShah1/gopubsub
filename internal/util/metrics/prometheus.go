package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// MetricsData holds the current metrics values
type MetricsData struct {
	MessagesPublished     int64
	MessagesReceived      int64
	MessagesFailed        int64
	SuccessfulOperations  int64
	FailedOperations      int64
	OperationDurations    map[string]time.Duration
	OperationDurationsAvg map[string]time.Duration
	OperationDurationsMax map[string]time.Duration
	OperationDurationsMin map[string]time.Duration
	OperationDurationsP95 map[string]time.Duration
	OperationDurationsP99 map[string]time.Duration
}

// Metrics defines the interface for metrics collection
type Metrics interface {
	// RecordMessagePublished increments the published messages counter
	RecordMessagePublished(provider string)
	// RecordMessageReceived increments the received messages counter
	RecordMessageReceived(provider string)
	// RecordMessageFailed increments the failed messages counter
	RecordMessageFailed(provider string)
	// RecordOperationDuration records the duration of an operation
	RecordOperationDuration(provider string, duration time.Duration, success bool)
	// GetMetrics returns the current metrics values
	GetMetrics() MetricsData
	// Reset resets all metrics to their initial values
	Reset()
	// RecordCircuitBreakerTrip records when the circuit breaker trips
	RecordCircuitBreakerTrip()
	// RecordCircuitBreakerReset records when the circuit breaker resets
	RecordCircuitBreakerReset()
}

// PrometheusConfig holds configuration for Prometheus metrics
type PrometheusConfig struct {
	// Namespace for all metrics
	Namespace string
	// Subsystem for all metrics
	Subsystem string
	// Bucket sizes for histograms
	Buckets []float64
}

// DefaultPrometheusConfig returns a PrometheusConfig with default values
func DefaultPrometheusConfig() *PrometheusConfig {
	return &PrometheusConfig{
		Namespace: "gopubsub",
		Subsystem: "pubsub",
		Buckets:   prometheus.DefBuckets,
	}
}

// PrometheusMetrics implements the Metrics interface using Prometheus
type PrometheusMetrics struct {
	// Configuration
	config *PrometheusConfig

	// Message counters
	messagesPublished *prometheus.CounterVec
	messagesReceived  *prometheus.CounterVec
	messagesFailed    *prometheus.CounterVec

	// Operation counters
	successfulOperations *prometheus.CounterVec
	failedOperations     *prometheus.CounterVec

	// Operation durations
	operationDurations *prometheus.HistogramVec

	// Circuit breaker metrics
	circuitBreakerTrips  *prometheus.CounterVec
	circuitBreakerResets *prometheus.CounterVec
}

// NewPrometheusMetrics creates a new PrometheusMetrics instance with default configuration
func NewPrometheusMetrics() *PrometheusMetrics {
	return NewPrometheusMetricsWithConfig(DefaultPrometheusConfig())
}

// NewPrometheusMetricsWithConfig creates a new PrometheusMetrics instance with custom configuration
func NewPrometheusMetricsWithConfig(config *PrometheusConfig) *PrometheusMetrics {
	if config == nil {
		config = DefaultPrometheusConfig()
	}

	return &PrometheusMetrics{
		config: config,
		messagesPublished: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "messages_published_total",
			Help:      "Total number of messages published",
		}, []string{"provider"}),
		messagesReceived: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "messages_received_total",
			Help:      "Total number of messages received",
		}, []string{"provider"}),
		messagesFailed: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "messages_failed_total",
			Help:      "Total number of messages that failed processing",
		}, []string{"provider"}),
		successfulOperations: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "operations_successful_total",
			Help:      "Total number of successful operations",
		}, []string{"provider"}),
		failedOperations: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "operations_failed_total",
			Help:      "Total number of failed operations",
		}, []string{"provider"}),
		operationDurations: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "operation_duration_seconds",
			Help:      "Duration of operations in seconds",
			Buckets:   config.Buckets,
		}, []string{"provider"}),
		circuitBreakerTrips: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "circuit_breaker_trips_total",
			Help:      "Total number of circuit breaker trips",
		}, []string{"provider"}),
		circuitBreakerResets: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "circuit_breaker_resets_total",
			Help:      "Total number of circuit breaker resets",
		}, []string{"provider"}),
	}
}

// RecordMessagePublished increments the published messages counter
func (m *PrometheusMetrics) RecordMessagePublished(provider string) {
	m.messagesPublished.WithLabelValues(provider).Inc()
}

// RecordMessageReceived increments the received messages counter
func (m *PrometheusMetrics) RecordMessageReceived(provider string) {
	m.messagesReceived.WithLabelValues(provider).Inc()
}

// RecordMessageFailed increments the failed messages counter
func (m *PrometheusMetrics) RecordMessageFailed(provider string) {
	m.messagesFailed.WithLabelValues(provider).Inc()
}

// RecordOperationDuration records the duration of an operation
func (m *PrometheusMetrics) RecordOperationDuration(provider string, duration time.Duration, success bool) {
	// Record operation success/failure
	if success {
		m.successfulOperations.WithLabelValues(provider).Inc()
	} else {
		m.failedOperations.WithLabelValues(provider).Inc()
	}

	// Record operation duration in seconds
	m.operationDurations.WithLabelValues(provider).Observe(duration.Seconds())
}

// RecordCircuitBreakerTrip records when the circuit breaker trips
func (m *PrometheusMetrics) RecordCircuitBreakerTrip() {
	m.circuitBreakerTrips.WithLabelValues("default").Inc()
}

// RecordCircuitBreakerReset records when the circuit breaker resets
func (m *PrometheusMetrics) RecordCircuitBreakerReset() {
	m.circuitBreakerResets.WithLabelValues("default").Inc()
}

// GetMetrics returns the current metrics values
func (m *PrometheusMetrics) GetMetrics() MetricsData {
	// For Prometheus metrics, we don't need to return the current values
	// as they are automatically collected by Prometheus
	return MetricsData{
		MessagesPublished:     0, // These values are not applicable for Prometheus metrics
		MessagesReceived:      0,
		MessagesFailed:        0,
		SuccessfulOperations:  0,
		FailedOperations:      0,
		OperationDurations:    make(map[string]time.Duration),
		OperationDurationsAvg: make(map[string]time.Duration),
		OperationDurationsMax: make(map[string]time.Duration),
		OperationDurationsMin: make(map[string]time.Duration),
		OperationDurationsP95: make(map[string]time.Duration),
		OperationDurationsP99: make(map[string]time.Duration),
	}
}

// Reset resets all metrics to their initial values
func (m *PrometheusMetrics) Reset() {
	// For Prometheus metrics, we don't need to reset the values
	// as they are automatically collected by Prometheus
}
