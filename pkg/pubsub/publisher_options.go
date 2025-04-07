package pubsub

import (
	"time"
)

// PublishOptions defines options for publishing
type PublishOptions struct {
	// Timeout for the publish operation
	Timeout time.Duration

	// Priority of the message (if supported by the broker)
	Priority int

	// DeliveryMode determines if messages are persistent or not
	Persistent bool

	// RetryConfig for publish operations
	RetryConfig RetryConfig

	// Additional broker-specific options stored as key-value pairs
	BrokerSpecificOpts map[string]interface{}
}

// RetryConfig defines retry behavior for operations
type RetryConfig struct {
	// MaxRetries is the maximum number of retries before giving up
	MaxRetries int

	// InitialBackoff is the initial backoff duration
	InitialBackoff time.Duration

	// MaxBackoff is the maximum backoff duration
	MaxBackoff time.Duration

	// BackoffMultiplier is the factor by which the backoff increases
	BackoffMultiplier float64
}

// PublishOption is a function that configures PublishOptions
type PublishOption func(*PublishOptions)

// defaultPublishOptions returns the default publish options
func defaultPublishOptions() *PublishOptions {
	return &PublishOptions{
		Timeout:    30 * time.Second,
		Priority:   0,
		Persistent: true,
		RetryConfig: RetryConfig{
			MaxRetries:        3,
			InitialBackoff:    100 * time.Millisecond,
			MaxBackoff:        10 * time.Second,
			BackoffMultiplier: 2.0,
		},
		BrokerSpecificOpts: make(map[string]interface{}),
	}
}

// WithPublishTimeout sets the timeout for publish operations
func WithPublishTimeout(timeout time.Duration) PublishOption {
	return func(o *PublishOptions) {
		o.Timeout = timeout
	}
}

// WithPriority sets the message priority if supported by the broker
func WithPriority(priority int) PublishOption {
	return func(o *PublishOptions) {
		o.Priority = priority
	}
}

// WithPersistence specifies whether messages should be stored persistently
func WithPersistence(persistent bool) PublishOption {
	return func(o *PublishOptions) {
		o.Persistent = persistent
	}
}

// WithPublishRetry configures retry behavior for publish operations
func WithPublishRetry(maxRetries int, initialBackoff time.Duration, maxBackoff time.Duration, multiplier float64) PublishOption {
	return func(o *PublishOptions) {
		o.RetryConfig = RetryConfig{
			MaxRetries:        maxRetries,
			InitialBackoff:    initialBackoff,
			MaxBackoff:        maxBackoff,
			BackoffMultiplier: multiplier,
		}
	}
}

// WithBrokerSpecificPublishOption adds a broker-specific option
func WithBrokerSpecificPublishOption(key string, value interface{}) PublishOption {
	return func(o *PublishOptions) {
		if o.BrokerSpecificOpts == nil {
			o.BrokerSpecificOpts = make(map[string]interface{})
		}
		o.BrokerSpecificOpts[key] = value
	}
}
