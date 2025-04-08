package pubsub

import (
	"encoding/json"
	"fmt"
)

// BrokerConfig is an interface that all broker-specific configurations must implement
type BrokerConfig interface {
	// Validate checks if the configuration is valid
	Validate() error
	// GetBrokerType returns the type of broker this configuration is for
	GetBrokerType() BrokerType
}

// RabbitMQConfig holds configuration for RabbitMQ broker
type RabbitMQConfig struct {
	// Host is the RabbitMQ server host
	Host string `json:"host"`
	// Port is the RabbitMQ server port
	Port int `json:"port"`
	// Username for authentication
	Username string `json:"username"`
	// Password for authentication
	Password string `json:"password"`
	// VirtualHost is the RabbitMQ virtual host
	VirtualHost string `json:"virtual_host"`
	// UseTLS indicates whether to use TLS for connection
	UseTLS bool `json:"use_tls"`
	// ExchangeName is the default exchange name
	ExchangeName string `json:"exchange_name"`
	// ExchangeType is the type of exchange (direct, fanout, topic, headers)
	ExchangeType string `json:"exchange_type"`
	// QueueName is the default queue name
	QueueName string `json:"queue_name"`
	// RoutingKey is the default routing key
	RoutingKey string `json:"routing_key"`
}

// Validate checks if the RabbitMQ configuration is valid
func (c *RabbitMQConfig) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("host cannot be empty")
	}
	if c.Port <= 0 {
		return fmt.Errorf("port must be positive")
	}
	if c.Username == "" {
		return fmt.Errorf("username cannot be empty")
	}
	if c.Password == "" {
		return fmt.Errorf("password cannot be empty")
	}
	if c.ExchangeName == "" {
		return fmt.Errorf("exchange name cannot be empty")
	}
	if c.ExchangeType == "" {
		return fmt.Errorf("exchange type cannot be empty")
	}
	if c.QueueName == "" {
		return fmt.Errorf("queue name cannot be empty")
	}
	return nil
}

// GetBrokerType returns the type of broker this configuration is for
func (c *RabbitMQConfig) GetBrokerType() BrokerType {
	return RabbitMQBroker
}

// KafkaConfig holds configuration for Kafka broker
type KafkaConfig struct {
	// Brokers is a list of Kafka broker addresses
	Brokers []string `json:"brokers"`
	// Topic is the default topic name
	Topic string `json:"topic"`
	// GroupID is the consumer group ID
	GroupID string `json:"group_id"`
	// ClientID is the client ID
	ClientID string `json:"client_id"`
	// UseTLS indicates whether to use TLS for connection
	UseTLS bool `json:"use_tls"`
	// SASLMechanism is the SASL mechanism to use
	SASLMechanism string `json:"sasl_mechanism"`
	// SASLUsername is the SASL username
	SASLUsername string `json:"sasl_username"`
	// SASLPassword is the SASL password
	SASLPassword string `json:"sasl_password"`
}

// Validate checks if the Kafka configuration is valid
func (c *KafkaConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("brokers cannot be empty")
	}
	if c.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	if c.GroupID == "" {
		return fmt.Errorf("group ID cannot be empty")
	}
	if c.ClientID == "" {
		return fmt.Errorf("client ID cannot be empty")
	}
	return nil
}

// GetBrokerType returns the type of broker this configuration is for
func (c *KafkaConfig) GetBrokerType() BrokerType {
	return KafkaBroker
}

// NATSConfig holds configuration for NATS broker
type NATSConfig struct {
	// Servers is a list of NATS server addresses
	Servers []string `json:"servers"`
	// Subject is the default subject name
	Subject string `json:"subject"`
	// QueueGroup is the queue group name for load balancing
	QueueGroup string `json:"queue_group"`
	// UseTLS indicates whether to use TLS for connection
	UseTLS bool `json:"use_tls"`
	// Username for authentication
	Username string `json:"username"`
	// Password for authentication
	Password string `json:"password"`
}

// Validate checks if the NATS configuration is valid
func (c *NATSConfig) Validate() error {
	if len(c.Servers) == 0 {
		return fmt.Errorf("servers cannot be empty")
	}
	if c.Subject == "" {
		return fmt.Errorf("subject cannot be empty")
	}
	return nil
}

// GetBrokerType returns the type of broker this configuration is for
func (c *NATSConfig) GetBrokerType() BrokerType {
	return NATSBroker
}

// DefaultBrokerConfig returns a default configuration for the specified broker type
func DefaultBrokerConfig(brokerType BrokerType) (BrokerConfig, error) {
	switch brokerType {
	case RabbitMQBroker:
		return &RabbitMQConfig{
			Host:         "localhost",
			Port:         5672,
			Username:     "guest",
			Password:     "guest",
			VirtualHost:  "/",
			UseTLS:       false,
			ExchangeName: "gopubsub",
			ExchangeType: "topic",
			QueueName:    "gopubsub",
			RoutingKey:   "#",
		}, nil
	case KafkaBroker:
		return &KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			Topic:         "gopubsub",
			GroupID:       "gopubsub",
			ClientID:      "gopubsub-client",
			UseTLS:        false,
			SASLMechanism: "PLAIN",
		}, nil
	case NATSBroker:
		return &NATSConfig{
			Servers:    []string{"nats://localhost:4222"},
			Subject:    "gopubsub",
			QueueGroup: "gopubsub",
			UseTLS:     false,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported broker type: %s", brokerType)
	}
}

// ParseBrokerConfig parses a JSON configuration into the appropriate broker config type
func ParseBrokerConfig(brokerType BrokerType, configJSON []byte) (BrokerConfig, error) {
	var config BrokerConfig

	switch brokerType {
	case RabbitMQBroker:
		config = &RabbitMQConfig{}
	case KafkaBroker:
		config = &KafkaConfig{}
	case NATSBroker:
		config = &NATSConfig{}
	default:
		return nil, fmt.Errorf("unsupported broker type: %s", brokerType)
	}

	if err := json.Unmarshal(configJSON, config); err != nil {
		return nil, fmt.Errorf("failed to parse broker config: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid broker config: %w", err)
	}

	return config, nil
}
