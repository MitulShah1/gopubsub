package pubsub

import (
	"context"
)

// Publisher defines the interface for publishing messages
type Publisher interface {
	// Publish sends a message to a topic
	Publish(ctx context.Context, topic string, msg *Message, opts ...PublishOption) error

	// PublishBatch sends multiple messages to a topic
	PublishBatch(ctx context.Context, topic string, msgs []*Message, opts ...PublishOption) error

	// Close closes the publisher and cleans up resources
	Close() error
}

// PublisherFactory is a function type for creating publishers
type PublisherFactory func(config map[string]interface{}) (Publisher, error)

// publisherFactories stores registered publisher factories by broker type
var publisherFactories = make(map[BrokerType]PublisherFactory)

// RegisterPublisherFactory registers a publisher factory for a broker type
func RegisterPublisherFactory(brokerType BrokerType, factory PublisherFactory) {
	publisherFactories[brokerType] = factory
}

// NewPublisher creates a new publisher for the specified broker type
func NewPublisher(brokerType BrokerType, config map[string]interface{}) (Publisher, error) {
	factory, exists := publisherFactories[brokerType]
	if !exists {
		return nil, NewError(
			ErrUnsupportedBroker,
			ErrorCodeConfiguration,
			"unsupported broker type",
			string(brokerType),
			false,
		)
	}

	return factory(config)
}
