package pubsub

import (
	"context"
	"fmt"
)

// MessageHandler is a function type for message processing
type MessageHandler func(ctx context.Context, msg *Message) error

// Subscriber defines the interface for consuming messages
type Subscriber interface {
	// Subscribe consumes messages from a topic or queue and processes them with the handler
	Subscribe(ctx context.Context, topic string, handler MessageHandler, opts ...SubscribeOption) error

	// Ack acknowledges that the message has been successfully processed
	Ack(ctx context.Context, msg *Message) error

	// Nack indicates that the message processing failed and should be handled
	// according to the broker's configuration (e.g., requeue, dead-letter, etc.)
	Nack(ctx context.Context, msg *Message, opts ...NackOption) error

	// Close closes the subscriber and cleans up resources
	Close() error
}

// SubscriberFactory is a function type for creating subscribers
type SubscriberFactory func(config BrokerConfig) (Subscriber, error)

// subscriberFactories stores registered subscriber factories by broker type
var subscriberFactories = make(map[BrokerType]SubscriberFactory)

// subscriberConfigValidators stores registered subscriber config validators by broker type
var subscriberConfigValidators = make(map[BrokerType]func(BrokerConfig) error)

// RegisterSubscriberFactory registers a subscriber factory for a broker type
func RegisterSubscriberFactory(brokerType BrokerType, factory SubscriberFactory) {
	subscriberFactories[brokerType] = factory
}

// RegisterSubscriberConfigValidator registers a subscriber config validator for a broker type
func RegisterSubscriberConfigValidator(brokerType BrokerType, validator func(BrokerConfig) error) {
	subscriberConfigValidators[brokerType] = validator
}

// NewSubscriber creates a new subscriber for the specified broker type
func NewSubscriber(brokerType BrokerType, config BrokerConfig) (Subscriber, error) {
	factory, exists := subscriberFactories[brokerType]
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
	if validator, exists := subscriberConfigValidators[brokerType]; exists {
		if err := validator(config); err != nil {
			return nil, NewError(
				err,
				ErrorCodeConfiguration,
				"invalid subscriber configuration",
				string(brokerType),
				false,
			)
		}
	}

	return factory(config)
}
