package pubsub

import (
	"context"
	"sync"
)

// Client combines Publisher and Subscriber interfaces
type Client interface {
	Publisher
	Subscriber
}

// BaseClient implements common functionality for all clients
type BaseClient struct {
	publisher  Publisher
	subscriber Subscriber
	brokerType BrokerType
	closed     bool
	mutex      sync.RWMutex
}

// NewClient creates a new Client for the specified broker type
func NewClient(brokerType BrokerType, config map[string]interface{}) (Client, error) {
	publisher, err := NewPublisher(brokerType, config)
	if err != nil {
		return nil, err
	}

	subscriber, err := NewSubscriber(brokerType, config)
	if err != nil {
		return nil, err
	}

	return &BaseClient{
		publisher:  publisher,
		subscriber: subscriber,
		brokerType: brokerType,
		closed:     false,
	}, nil
}

// Publish delegates to the underlying publisher
func (c *BaseClient) Publish(ctx context.Context, topic string, msg *Message, opts ...PublishOption) error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.closed {
		return NewError(
			ErrAlreadyClosed,
			ErrorCodeConfiguration,
			"client is closed",
			string(c.brokerType),
			false,
		)
	}

	return c.publisher.Publish(ctx, topic, msg, opts...)
}

// PublishBatch delegates to the underlying publisher
func (c *BaseClient) PublishBatch(ctx context.Context, topic string, msgs []*Message, opts ...PublishOption) error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.closed {
		return NewError(
			ErrAlreadyClosed,
			ErrorCodeConfiguration,
			"client is closed",
			string(c.brokerType),
			false,
		)
	}

	return c.publisher.PublishBatch(ctx, topic, msgs, opts...)
}

// Subscribe delegates to the underlying subscriber
func (c *BaseClient) Subscribe(ctx context.Context, topic string, handler MessageHandler, opts ...SubscribeOption) error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.closed {
		return NewError(
			ErrAlreadyClosed,
			ErrorCodeConfiguration,
			"client is closed",
			string(c.brokerType),
			false,
		)
	}

	return c.subscriber.Subscribe(ctx, topic, handler, opts...)
}

// Ack delegates to the underlying subscriber
func (c *BaseClient) Ack(ctx context.Context, msg *Message) error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.closed {
		return NewError(
			ErrAlreadyClosed,
			ErrorCodeConfiguration,
			"client is closed",
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
			"client is closed",
			string(c.brokerType),
			false,
		)
	}

	return c.subscriber.Nack(ctx, msg, opts...)
}

// Close closes both the publisher and subscriber
func (c *BaseClient) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.closed {
		return nil
	}
	return c.publisher.Close()
}
