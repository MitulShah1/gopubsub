package pubsub

import (
	"context"
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
type SubscriberFactory func(config map[string]interface{}) (Subscriber, error)

// subscriberFactories stores registered subscriber factories by broker type
var subscriberFactories = make(map[BrokerType]SubscriberFactory)

// RegisterSubscriberFactory registers a subscriber factory for a broker type
func RegisterSubscriberFactory(brokerType BrokerType, factory SubscriberFactory) {
	subscriberFactories[brokerType] = factory
}

// NewSubscriber creates a new subscriber for the specified broker type
func NewSubscriber(brokerType BrokerType, config map[string]interface{}) (Subscriber, error) {
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

	return factory(config)
}
