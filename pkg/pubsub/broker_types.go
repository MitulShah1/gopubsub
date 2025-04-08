package pubsub

// BrokerType represents the type of message broker
type BrokerType string

const (
	// RabbitMQBroker represents a RabbitMQ broker
	RabbitMQBroker BrokerType = "rabbitmq"
	// KafkaBroker represents a Kafka broker
	KafkaBroker BrokerType = "kafka"
	// NATSBroker represents a NATS broker
	NATSBroker BrokerType = "nats"
)

// String returns the string representation of the broker type
func (b BrokerType) String() string {
	return string(b)
}

// IsValid checks if the broker type is supported
func (b BrokerType) IsValid() bool {
	switch b {
	case RabbitMQBroker, KafkaBroker, NATSBroker:
		return true
	default:
		return false
	}
}
