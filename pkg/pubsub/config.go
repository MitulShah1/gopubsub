package pubsub

import (
	"errors"
	"time"

	"github.com/MitulShah1/gopubsub/internal/util/failover"
	"github.com/MitulShah1/gopubsub/internal/util/logger"
	"github.com/MitulShah1/gopubsub/internal/util/metrics"
)

// ClientConfig holds all configuration options for a pubsub client
type ClientConfig struct {
	// Broker configuration
	BrokerConfig BrokerConfig `json:"broker_config"`

	// Retry configuration
	RetryConfig *failover.RetryConfig `json:"retry_config,omitempty"`

	// Circuit breaker configuration
	CircuitBreakerConfig *failover.CircuitBreakerConfig `json:"circuit_breaker_config,omitempty"`

	// Metrics configuration
	MetricsConfig *MetricsConfig `json:"metrics_config,omitempty"`

	// Logger configuration
	LoggerConfig *LoggerConfig `json:"logger_config,omitempty"`

	// Serialization configuration
	SerializationConfig *SerializationConfig `json:"serialization_config,omitempty"`
}

// MetricsConfig holds configuration for metrics collection
type MetricsConfig struct {
	// Whether to enable metrics collection
	Enabled bool `json:"enabled"`

	// Custom metrics instance (if nil, a new one will be created)
	Metrics *metrics.PrometheusMetrics `json:"-"`

	// Prometheus configuration
	PrometheusConfig *metrics.PrometheusConfig `json:"prometheus_config,omitempty"`
}

// LoggerConfig holds configuration for logging
type LoggerConfig struct {
	// Log level (0=Debug, 1=Info, 2=Warn, 3=Error)
	Level logger.Level `json:"level"`

	// Custom logger instance (if nil, a new one will be created)
	Logger logger.Logger `json:"-"`
}

// SerializationConfig holds configuration for message serialization
type SerializationConfig struct {
	// Serialization format (json, protobuf, avro)
	Format string `json:"format"`

	// Whether to compress messages
	Compression bool `json:"compression"`
}

// DefaultClientConfig returns a ClientConfig with default values
func DefaultClientConfig(brokerType BrokerType) (*ClientConfig, error) {
	brokerConfig, err := DefaultBrokerConfig(brokerType)
	if err != nil {
		return nil, err
	}

	return &ClientConfig{
		BrokerConfig: brokerConfig,
		RetryConfig: &failover.RetryConfig{
			MaxRetries:   3,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     time.Second,
			Multiplier:   2.0,
			JitterFactor: 0.1,
		},
		CircuitBreakerConfig: &failover.CircuitBreakerConfig{
			FailureThreshold: 5,
			ResetTimeout:     time.Second,
			HalfOpenRequests: 3,
		},
		MetricsConfig: &MetricsConfig{
			Enabled: true,
			PrometheusConfig: &metrics.PrometheusConfig{
				Namespace: "gopubsub",
				Subsystem: "pubsub",
				Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			},
		},
		LoggerConfig: &LoggerConfig{
			Level: logger.InfoLevel,
		},
		SerializationConfig: &SerializationConfig{
			Format:      "json",
			Compression: false,
		},
	}, nil
}

// Validate checks if the client configuration is valid
func (c *ClientConfig) Validate() error {
	if c == nil {
		return NewError(
			errors.New("invalid client configuration"),
			ErrorCodeConfiguration,
			"configuration cannot be nil",
			"",
			false,
		)
	}

	if c.BrokerConfig == nil {
		return NewError(
			errors.New("broker configuration cannot be nil"),
			ErrorCodeConfiguration,
			"broker configuration cannot be nil",
			"",
			false,
		)
	}

	// Validate broker configuration
	if err := c.BrokerConfig.Validate(); err != nil {
		return NewError(
			err,
			ErrorCodeConfiguration,
			"invalid broker configuration",
			"",
			false,
		)
	}

	// Validate retry configuration if present
	if c.RetryConfig != nil {
		if err := c.RetryConfig.Validate(); err != nil {
			return err
		}
	}

	// Validate circuit breaker configuration if present
	if c.CircuitBreakerConfig != nil {
		if err := c.CircuitBreakerConfig.Validate(); err != nil {
			return err
		}
	}

	return nil
}
