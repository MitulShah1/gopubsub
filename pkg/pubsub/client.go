package pubsub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/MitulShah1/gopubsub/internal/util/failover"
	"github.com/MitulShah1/gopubsub/internal/util/metrics"
)

// Client combines Publisher and Subscriber interfaces
// It provides a unified interface for both publishing and subscribing to messages
type Client interface {
	Publisher
	Subscriber
}

// BaseClient implements common functionality for all clients
// It maintains both publisher and subscriber instances and handles their lifecycle
type BaseClient struct {
	publisher  Publisher
	subscriber Subscriber
	brokerType BrokerType
	config     *ClientConfig
	closed     bool
	mutex      sync.RWMutex
}

// NewClient creates a new Client for the specified broker type
// It initializes both publisher and subscriber with the provided configuration
// The client supports retry, circuit breaker, and metrics if configured
func NewClient(brokerType BrokerType, config *ClientConfig) (Client, error) {
	if config == nil {
		var err error
		config, err = DefaultClientConfig(brokerType)
		if err != nil {
			return nil, err
		}
	}

	// Validate broker type matches configuration
	if config.BrokerConfig.GetBrokerType() != brokerType {
		return nil, NewError(
			fmt.Errorf("broker type mismatch: expected %s, got %s", brokerType, config.BrokerConfig.GetBrokerType()),
			ErrorCodeConfiguration,
			"broker type mismatch",
			"",
			false,
		)
	}

	// Initialize metrics if enabled
	if config.MetricsConfig != nil && config.MetricsConfig.Enabled && config.MetricsConfig.Metrics == nil {
		if config.MetricsConfig.PrometheusConfig != nil {
			config.MetricsConfig.Metrics = metrics.NewPrometheusMetricsWithConfig(config.MetricsConfig.PrometheusConfig)
		} else {
			config.MetricsConfig.Metrics = metrics.NewPrometheusMetrics()
		}
	}

	// Create publisher with retry and circuit breaker
	publisher, err := NewPublisher(brokerType, config.BrokerConfig)
	if err != nil {
		return nil, err
	}

	// Create subscriber with retry and circuit breaker
	subscriber, err := NewSubscriber(brokerType, config.BrokerConfig)
	if err != nil {
		return nil, err
	}

	return &BaseClient{
		publisher:  publisher,
		subscriber: subscriber,
		brokerType: brokerType,
		config:     config,
		closed:     false,
	}, nil
}

// NewPublisherClient creates a new Client that only implements the Publisher interface
// It's useful when you only need to publish messages and don't need subscription functionality
// The client supports retry, circuit breaker, and metrics if configured
func NewPublisherClient(brokerType BrokerType, config *ClientConfig) (Publisher, error) {
	if config == nil {
		var err error
		config, err = DefaultClientConfig(brokerType)
		if err != nil {
			return nil, err
		}
	}

	// Validate broker type matches configuration
	if config.BrokerConfig.GetBrokerType() != brokerType {
		return nil, NewError(
			fmt.Errorf("broker type mismatch: expected %s, got %s", brokerType, config.BrokerConfig.GetBrokerType()),
			ErrorCodeConfiguration,
			"broker type mismatch",
			"",
			false,
		)
	}

	// Initialize metrics if enabled
	if config.MetricsConfig != nil && config.MetricsConfig.Enabled && config.MetricsConfig.Metrics == nil {
		if config.MetricsConfig.PrometheusConfig != nil {
			config.MetricsConfig.Metrics = metrics.NewPrometheusMetricsWithConfig(config.MetricsConfig.PrometheusConfig)
		} else {
			config.MetricsConfig.Metrics = metrics.NewPrometheusMetrics()
		}
	}

	// Create publisher with retry and circuit breaker
	publisher, err := NewPublisher(brokerType, config.BrokerConfig)
	if err != nil {
		return nil, err
	}

	return &PublisherClient{
		publisher:  publisher,
		brokerType: brokerType,
		config:     config,
		closed:     false,
		mutex:      sync.RWMutex{},
	}, nil
}

// NewSubscriberClient creates a new Client that only implements the Subscriber interface
// It's useful when you only need to subscribe to messages and don't need publishing functionality
// The client supports retry, circuit breaker, and metrics if configured
func NewSubscriberClient(brokerType BrokerType, config *ClientConfig) (Subscriber, error) {
	if config == nil {
		var err error
		config, err = DefaultClientConfig(brokerType)
		if err != nil {
			return nil, err
		}
	}

	// Validate broker type matches configuration
	if config.BrokerConfig.GetBrokerType() != brokerType {
		return nil, NewError(
			fmt.Errorf("broker type mismatch: expected %s, got %s", brokerType, config.BrokerConfig.GetBrokerType()),
			ErrorCodeConfiguration,
			"broker type mismatch",
			"",
			false,
		)
	}

	// Initialize metrics if enabled
	if config.MetricsConfig != nil && config.MetricsConfig.Enabled && config.MetricsConfig.Metrics == nil {
		if config.MetricsConfig.PrometheusConfig != nil {
			config.MetricsConfig.Metrics = metrics.NewPrometheusMetricsWithConfig(config.MetricsConfig.PrometheusConfig)
		} else {
			config.MetricsConfig.Metrics = metrics.NewPrometheusMetrics()
		}
	}

	// Create subscriber with retry and circuit breaker
	subscriber, err := NewSubscriber(brokerType, config.BrokerConfig)
	if err != nil {
		return nil, err
	}

	return &SubscriberClient{
		subscriber: subscriber,
		brokerType: brokerType,
		config:     config,
		closed:     false,
		mutex:      sync.RWMutex{},
	}, nil
}

// PublisherClient implements only the Publisher interface
type PublisherClient struct {
	publisher  Publisher
	brokerType BrokerType
	config     *ClientConfig
	closed     bool
	mutex      sync.RWMutex
}

// Publish delegates to the underlying publisher with retry and circuit breaker
func (c *PublisherClient) Publish(ctx context.Context, topic string, msg *Message, opts ...PublishOption) error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.closed {
		return NewError(
			errors.New("client is already closed"),
			ErrorCodeConfiguration,
			ErrMsgClientClosed,
			string(c.brokerType),
			false,
		)
	}

	// Check context before proceeding
	select {
	case <-ctx.Done():
		if c.config.MetricsConfig != nil && c.config.MetricsConfig.Enabled {
			c.config.MetricsConfig.Metrics.RecordMessageFailed(string(c.brokerType))
		}
		return ctx.Err()
	default:
	}

	startTime := time.Now()
	var err error

	// Apply publish options
	publishOpts := ApplyPublishOptions(opts...)

	// Use retry config from options if provided, otherwise use client config
	retryConfig := publishOpts.RetryConfig
	if retryConfig == nil && c.config.RetryConfig != nil {
		retryConfig = c.config.RetryConfig
	}

	// Apply retry and circuit breaker if configured
	if retryConfig != nil {
		err = failover.ExecuteWithRetry(ctx, retryConfig, func() error {
			// Check context before each retry attempt
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return c.publisher.Publish(ctx, topic, msg, opts...)
			}
		})
	} else {
		err = c.publisher.Publish(ctx, topic, msg, opts...)
	}

	// Record metrics
	if c.config.MetricsConfig != nil && c.config.MetricsConfig.Enabled {
		if err != nil {
			c.config.MetricsConfig.Metrics.RecordMessageFailed(string(c.brokerType))
			c.config.MetricsConfig.Metrics.RecordOperationDuration(string(c.brokerType), time.Since(startTime), false)
		} else {
			c.config.MetricsConfig.Metrics.RecordMessagePublished(string(c.brokerType))
			c.config.MetricsConfig.Metrics.RecordOperationDuration(string(c.brokerType), time.Since(startTime), true)
		}
	}

	return err
}

// PublishBatch delegates to the underlying publisher
func (c *PublisherClient) PublishBatch(ctx context.Context, topic string, msgs []*Message, opts ...PublishOption) error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.closed {
		return NewError(
			ErrAlreadyClosed,
			ErrorCodeConfiguration,
			"client is already closed",
			string(c.brokerType),
			false,
		)
	}

	return c.publisher.PublishBatch(ctx, topic, msgs, opts...)
}

// Close closes the client and its underlying publisher
func (c *PublisherClient) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	var errs []error

	if err := c.publisher.Close(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.New("failed to close some resources")
	}

	return nil
}

// SubscriberClient implements only the Subscriber interface
type SubscriberClient struct {
	subscriber Subscriber
	brokerType BrokerType
	config     *ClientConfig
	closed     bool
	mutex      sync.RWMutex
}

// Subscribe delegates to the underlying subscriber with retry and circuit breaker
func (c *SubscriberClient) Subscribe(ctx context.Context, topic string, handler MessageHandler, opts ...SubscribeOption) error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.closed {
		return NewError(
			ErrAlreadyClosed,
			ErrorCodeConfiguration,
			"client is already closed",
			string(c.brokerType),
			false,
		)
	}

	// Apply subscribe options
	subscribeOpts := ApplySubscribeOptions(opts...)

	// Use retry config from options if provided, otherwise use client config
	retryConfig := subscribeOpts.RetryConfig
	if retryConfig == nil && c.config.RetryConfig != nil {
		retryConfig = c.config.RetryConfig
	}

	// Wrap the handler with retry and circuit breaker if configured
	wrappedHandler := handler
	if retryConfig != nil {
		wrappedHandler = func(ctx context.Context, msg *Message) error {
			var err error
			err = failover.ExecuteWithRetry(ctx, retryConfig, func() error {
				return handler(ctx, msg)
			})
			if err != nil {
				if c.config.MetricsConfig != nil && c.config.MetricsConfig.Enabled {
					c.config.MetricsConfig.Metrics.RecordMessageFailed(string(c.brokerType))
				}
				return err
			}
			return nil
		}
	}

	if err := c.subscriber.Subscribe(ctx, topic, wrappedHandler, opts...); err != nil {
		if c.config.MetricsConfig != nil && c.config.MetricsConfig.Enabled {
			c.config.MetricsConfig.Metrics.RecordMessageFailed(string(c.brokerType))
		}
		return err
	}

	if c.config.MetricsConfig != nil && c.config.MetricsConfig.Enabled {
		c.config.MetricsConfig.Metrics.RecordMessageReceived(string(c.brokerType))
	}

	return nil
}

// Ack delegates to the underlying subscriber
func (c *SubscriberClient) Ack(ctx context.Context, msg *Message) error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.closed {
		return NewError(
			ErrAlreadyClosed,
			ErrorCodeConfiguration,
			"client is already closed",
			string(c.brokerType),
			false,
		)
	}

	return c.subscriber.Ack(ctx, msg)
}

// Nack delegates to the underlying subscriber
func (c *SubscriberClient) Nack(ctx context.Context, msg *Message, opts ...NackOption) error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.closed {
		return NewError(
			ErrAlreadyClosed,
			ErrorCodeConfiguration,
			"client is already closed",
			string(c.brokerType),
			false,
		)
	}

	return c.subscriber.Nack(ctx, msg, opts...)
}

// Close closes the client and its underlying subscriber
func (c *SubscriberClient) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	var errs []error

	if err := c.subscriber.Close(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.New("failed to close some resources")
	}

	return nil
}

// Publish delegates to the underlying publisher with retry and circuit breaker
func (c *BaseClient) Publish(ctx context.Context, topic string, msg *Message, opts ...PublishOption) error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.closed {
		return NewError(
			ErrAlreadyClosed,
			ErrorCodeConfiguration,
			"client is already closed",
			string(c.brokerType),
			false,
		)
	}

	// Apply publish options
	publishOpts := ApplyPublishOptions(opts...)

	// Use retry config from options if provided, otherwise use client config
	retryConfig := publishOpts.RetryConfig
	if retryConfig == nil && c.config.RetryConfig != nil {
		retryConfig = c.config.RetryConfig
	}

	// Apply retry and circuit breaker if configured
	if retryConfig != nil {
		var err error
		err = failover.ExecuteWithRetry(ctx, retryConfig, func() error {
			return c.publisher.Publish(ctx, topic, msg, opts...)
		})
		if err != nil {
			if c.config.MetricsConfig != nil && c.config.MetricsConfig.Enabled {
				c.config.MetricsConfig.Metrics.RecordMessageFailed(string(c.brokerType))
			}
			return err
		}
	} else {
		if err := c.publisher.Publish(ctx, topic, msg, opts...); err != nil {
			if c.config.MetricsConfig != nil && c.config.MetricsConfig.Enabled {
				c.config.MetricsConfig.Metrics.RecordMessageFailed(string(c.brokerType))
			}
			return err
		}
	}

	if c.config.MetricsConfig != nil && c.config.MetricsConfig.Enabled {
		c.config.MetricsConfig.Metrics.RecordMessagePublished(string(c.brokerType))
	}

	return nil
}

// PublishBatch delegates to the underlying publisher
func (c *BaseClient) PublishBatch(ctx context.Context, topic string, msgs []*Message, opts ...PublishOption) error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.closed {
		return NewError(
			ErrAlreadyClosed,
			ErrorCodeConfiguration,
			"client is already closed",
			string(c.brokerType),
			false,
		)
	}

	return c.publisher.PublishBatch(ctx, topic, msgs, opts...)
}

// Subscribe delegates to the underlying subscriber with retry and circuit breaker
func (c *BaseClient) Subscribe(ctx context.Context, topic string, handler MessageHandler, opts ...SubscribeOption) error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.closed {
		return NewError(
			ErrAlreadyClosed,
			ErrorCodeConfiguration,
			"client is already closed",
			string(c.brokerType),
			false,
		)
	}

	// Apply subscribe options
	subscribeOpts := ApplySubscribeOptions(opts...)

	// Use retry config from options if provided, otherwise use client config
	retryConfig := subscribeOpts.RetryConfig
	if retryConfig == nil && c.config.RetryConfig != nil {
		retryConfig = c.config.RetryConfig
	}

	// Wrap the handler with retry and circuit breaker if configured
	wrappedHandler := handler
	if retryConfig != nil {
		wrappedHandler = func(ctx context.Context, msg *Message) error {
			var err error
			err = failover.ExecuteWithRetry(ctx, retryConfig, func() error {
				return handler(ctx, msg)
			})
			if err != nil {
				if c.config.MetricsConfig != nil && c.config.MetricsConfig.Enabled {
					c.config.MetricsConfig.Metrics.RecordMessageFailed(string(c.brokerType))
				}
				return err
			}
			return nil
		}
	}

	if err := c.subscriber.Subscribe(ctx, topic, wrappedHandler, opts...); err != nil {
		if c.config.MetricsConfig != nil && c.config.MetricsConfig.Enabled {
			c.config.MetricsConfig.Metrics.RecordMessageFailed(string(c.brokerType))
		}
		return err
	}

	if c.config.MetricsConfig != nil && c.config.MetricsConfig.Enabled {
		c.config.MetricsConfig.Metrics.RecordMessageReceived(string(c.brokerType))
	}

	return nil
}

// Ack delegates to the underlying subscriber
func (c *BaseClient) Ack(ctx context.Context, msg *Message) error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.closed {
		return NewError(
			ErrAlreadyClosed,
			ErrorCodeConfiguration,
			"client is already closed",
			string(c.brokerType),
			false,
		)
	}

	return c.subscriber.Ack(ctx, msg)
}

// Nack delegates to the underlying subscriber
func (c *BaseClient) Nack(ctx context.Context, msg *Message, opts ...NackOption) error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.closed {
		return NewError(
			ErrAlreadyClosed,
			ErrorCodeConfiguration,
			"client is already closed",
			string(c.brokerType),
			false,
		)
	}

	return c.subscriber.Nack(ctx, msg, opts...)
}

// Close closes the client and its underlying publisher and subscriber
func (c *BaseClient) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	var errs []error

	if err := c.publisher.Close(); err != nil {
		errs = append(errs, err)
	}

	if err := c.subscriber.Close(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.New("failed to close some resources")
	}

	return nil
}

// ClientFactory is a function type for creating clients
type ClientFactory func(config map[string]interface{}) (Client, error)

// clientFactories stores registered client factories by broker type
var clientFactories = make(map[BrokerType]ClientFactory)

// RegisterClientFactory registers a client factory for a broker type
func RegisterClientFactory(brokerType BrokerType, factory ClientFactory) {
	clientFactories[brokerType] = factory
}

// GetSupportedBrokers returns a list of all supported broker types
func GetSupportedBrokers() []BrokerType {
	brokers := make([]BrokerType, 0, len(clientFactories))
	for broker := range clientFactories {
		brokers = append(brokers, broker)
	}
	return brokers
}

// IsBrokerSupported checks if a broker type is supported
func IsBrokerSupported(brokerType BrokerType) bool {
	_, exists := clientFactories[brokerType]
	return exists
}
