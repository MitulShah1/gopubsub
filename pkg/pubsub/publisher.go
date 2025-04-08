package pubsub

import (
	"context"
	"fmt"
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
type PublisherFactory func(config BrokerConfig) (Publisher, error)

// publisherFactories stores registered publisher factories by broker type
var publisherFactories = make(map[BrokerType]PublisherFactory)

// publisherConfigValidators stores registered publisher config validators by broker type
var publisherConfigValidators = make(map[BrokerType]func(BrokerConfig) error)

// RegisterPublisherFactory registers a publisher factory for a broker type
func RegisterPublisherFactory(brokerType BrokerType, factory PublisherFactory) {
	publisherFactories[brokerType] = factory
}

// RegisterPublisherConfigValidator registers a publisher config validator for a broker type
func RegisterPublisherConfigValidator(brokerType BrokerType, validator func(BrokerConfig) error) {
	publisherConfigValidators[brokerType] = validator
}

// NewPublisher creates a new publisher for the specified broker type
func NewPublisher(brokerType BrokerType, config BrokerConfig) (Publisher, error) {
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

	// Validate broker type matches configuration
	if config.GetBrokerType() != brokerType {
		return nil, NewError(
			fmt.Errorf("broker type mismatch: expected %s, got %s", brokerType, config.GetBrokerType()),
			ErrorCodeConfiguration,
			"broker type mismatch",
			"",
			false,
		)
	}

	// Validate configuration if validator exists
	if validator, exists := publisherConfigValidators[brokerType]; exists {
		if err := validator(config); err != nil {
			return nil, NewError(
				err,
				ErrorCodeConfiguration,
				"invalid publisher configuration",
				string(brokerType),
				false,
			)
		}
	}

	return factory(config)
}
