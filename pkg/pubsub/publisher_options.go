package pubsub

import (
	"time"

	"github.com/MitulShah1/gopubsub/internal/util/failover"
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
	RetryConfig *failover.RetryConfig

	// Additional broker-specific options stored as key-value pairs
	BrokerSpecificOpts map[string]interface{}
}

// PublishOption is a function that configures PublishOptions
type PublishOption func(*PublishOptions)

// defaultPublishOptions returns the default publish options
func defaultPublishOptions() *PublishOptions {
	return &PublishOptions{
		Timeout:    30 * time.Second,
		Priority:   0,
		Persistent: true,
		RetryConfig: &failover.RetryConfig{
			MaxRetries:   3,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     time.Second,
			Multiplier:   2.0,
			JitterFactor: 0.1,
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
func WithPublishRetry(maxRetries int, initialDelay time.Duration, maxDelay time.Duration, multiplier float64, jitterFactor float64) PublishOption {
	return func(o *PublishOptions) {
		o.RetryConfig = &failover.RetryConfig{
			MaxRetries:   maxRetries,
			InitialDelay: initialDelay,
			MaxDelay:     maxDelay,
			Multiplier:   multiplier,
			JitterFactor: jitterFactor,
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

// ApplyPublishOptions applies a list of PublishOption functions to the default options
func ApplyPublishOptions(opts ...PublishOption) *PublishOptions {
	options := defaultPublishOptions()
	for _, opt := range opts {
		opt(options)
	}
	return options
}
