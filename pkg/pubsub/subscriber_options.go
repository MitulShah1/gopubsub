package pubsub

import (
	"time"
)

// SubscribeOptions defines options for subscription
type SubscribeOptions struct {
	// ConsumerGroup defines a group of consumers that share message processing
	ConsumerGroup string

	// AutoAck determines if messages should be automatically acknowledged
	AutoAck bool

	// PrefetchCount sets how many messages to prefetch (if supported)
	PrefetchCount int

	// Concurrency defines how many messages can be processed concurrently
	Concurrency int

	// DeadLetterConfig contains dead letter queue configuration
	DeadLetterConfig *DeadLetterConfig

	// Durable subscriptions survive client disconnects
	Durable bool

	// Filter allows filtering messages using broker-specific mechanisms
	Filter string

	// Offset defines where to start consuming messages from
	Offset SubscriptionOffset

	// RetryConfig defines retry behavior for subscribe operations
	RetryConfig RetryConfig

	// Additional broker-specific options
	BrokerSpecificOpts map[string]interface{}
}

// DeadLetterConfig contains configuration for dead letter queues
type DeadLetterConfig struct {
	// Topic is where messages go after repeated failures
	Topic string

	// MaxRetries before sending to dead letter queue
	MaxRetries int

	// Enable indicates whether to use dead letter functionality
	Enable bool
}

// SubscriptionOffset represents different starting positions for consumption
type SubscriptionOffset string

const (
	// OffsetEarliest starts from the earliest message available
	OffsetEarliest SubscriptionOffset = "earliest"

	// OffsetLatest starts from the latest message
	OffsetLatest SubscriptionOffset = "latest"

	// OffsetTimestamp starts from a specific timestamp
	OffsetTimestamp SubscriptionOffset = "timestamp"
)

// SubscribeOption is a function that configures SubscribeOptions
type SubscribeOption func(*SubscribeOptions)

// defaultSubscribeOptions returns the default subscribe options
func defaultSubscribeOptions() *SubscribeOptions {
	return &SubscribeOptions{
		ConsumerGroup: "",
		AutoAck:       true,
		PrefetchCount: 10,
		Concurrency:   1,
		DeadLetterConfig: &DeadLetterConfig{
			Topic:      "",
			MaxRetries: 3,
			Enable:     false,
		},
		Durable:            true,
		Filter:             "",
		Offset:             OffsetLatest,
		RetryConfig:        defaultRetryConfig(),
		BrokerSpecificOpts: make(map[string]interface{}),
	}
}

// defaultRetryConfig returns the default retry configuration
func defaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:        3,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        10 * time.Second,
		BackoffMultiplier: 2.0,
	}
}

// WithConsumerGroup sets the consumer group name
func WithConsumerGroup(group string) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.ConsumerGroup = group
	}
}

// WithAutoAck enables or disables automatic acknowledgment
func WithAutoAck(autoAck bool) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.AutoAck = autoAck
	}
}

// WithPrefetchCount sets the number of messages to prefetch
func WithPrefetchCount(count int) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.PrefetchCount = count
	}
}

// WithConcurrency sets how many messages can be processed concurrently
func WithConcurrency(concurrency int) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Concurrency = concurrency
	}
}

// WithDeadLetter configures dead letter handling
func WithDeadLetter(topic string, maxRetries int) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.DeadLetterConfig = &DeadLetterConfig{
			Topic:      topic,
			MaxRetries: maxRetries,
			Enable:     true,
		}
	}
}

// WithDurable makes the subscription durable
func WithDurable(durable bool) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Durable = durable
	}
}

// WithFilter sets a filter for the subscription
func WithFilter(filter string) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Filter = filter
	}
}

// WithOffset sets the starting offset for the subscription
func WithOffset(offset SubscriptionOffset) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Offset = offset
	}
}

// WithSubscriptionRetry configures retry behavior for subscription operations
func WithSubscriptionRetry(maxRetries int, initialBackoff time.Duration, maxBackoff time.Duration, multiplier float64) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.RetryConfig = RetryConfig{
			MaxRetries:        maxRetries,
			InitialBackoff:    initialBackoff,
			MaxBackoff:        maxBackoff,
			BackoffMultiplier: multiplier,
		}
	}
}

// WithBrokerSpecificSubscribeOption adds a broker-specific option
func WithBrokerSpecificSubscribeOption(key string, value interface{}) SubscribeOption {
	return func(o *SubscribeOptions) {
		if o.BrokerSpecificOpts == nil {
			o.BrokerSpecificOpts = make(map[string]interface{})
		}
		o.BrokerSpecificOpts[key] = value
	}
}

// NackOptions defines options for negative acknowledgment
type NackOptions struct {
	// Requeue determines if the message should be requeued
	Requeue bool

	// RequeueDelay specifies a delay before requeuing
	RequeueDelay time.Duration

	// AdditionalInfo provides additional context for the Nack
	AdditionalInfo string

	// BrokerSpecificOpts contains broker-specific options
	BrokerSpecificOpts map[string]interface{}
}

// NackOption is a function that configures NackOptions
type NackOption func(*NackOptions)

// defaultNackOptions returns the default nack options
func defaultNackOptions() *NackOptions {
	return &NackOptions{
		Requeue:            true,
		RequeueDelay:       0,
		AdditionalInfo:     "",
		BrokerSpecificOpts: make(map[string]interface{}),
	}
}

// WithRequeue determines if the message should be requeued
func WithRequeue(requeue bool) NackOption {
	return func(o *NackOptions) {
		o.Requeue = requeue
	}
}

// WithRequeueDelay sets a delay before requeuing
func WithRequeueDelay(delay time.Duration) NackOption {
	return func(o *NackOptions) {
		o.RequeueDelay = delay
	}
}

// WithNackInfo provides additional context for the Nack
func WithNackInfo(info string) NackOption {
	return func(o *NackOptions) {
		o.AdditionalInfo = info
	}
}

// WithBrokerSpecificNackOption adds a broker-specific option
func WithBrokerSpecificNackOption(key string, value interface{}) NackOption {
	return func(o *NackOptions) {
		if o.BrokerSpecificOpts == nil {
			o.BrokerSpecificOpts = make(map[string]interface{})
		}
		o.BrokerSpecificOpts[key] = value
	}
}

// applySubscribeOptions applies the provided options to the default options
func applySubscribeOptions(opts ...SubscribeOption) *SubscribeOptions {
	options := defaultSubscribeOptions()
	for _, opt := range opts {
		opt(options)
	}
	return options
}

// applyNackOptions applies the provided options to the default options
func applyNackOptions(opts ...NackOption) *NackOptions {
	options := defaultNackOptions()
	for _, opt := range opts {
		opt(options)
	}
	return options
}
