package rabbitmq

import (
	"github.com/MitulShah1/gopubsub/internal/util/validation"
	"github.com/MitulShah1/gopubsub/pkg/pubsub"
)

// ValidateConfig validates the RabbitMQ configuration
func ValidateConfig(config *Config) error {
	v := validation.NewValidator()

	// Required fields
	v.Required("URI", config.URI)
	v.URL("URI", config.URI)

	// Duration fields
	v.Duration("ConnectionTimeout", config.ConnectionTimeout)
	v.Duration("ReconnectDelay", config.ReconnectDelay)

	// Numeric fields
	v.Min("MaxReconnect", config.MaxReconnect, 1)

	// Exchange type validation
	if config.DefaultExchangeType != "" {
		v.OneOf("DefaultExchangeType", config.DefaultExchangeType, "direct", "fanout", "topic", "headers")
	}

	// Log level validation
	if config.LogLevel != 0 {
		v.OneOf("LogLevel", config.LogLevel, 0, 1, 2, 3) // Debug, Info, Warn, Error
	}

	return v.Validate()
}

// ValidatePublisherConfig validates the publisher configuration
func ValidatePublisherConfig(config map[string]interface{}) error {
	// Extract RabbitMQ specific config
	rabbitConfig, ok := config["rabbitmq"].(map[string]interface{})
	if !ok {
		return pubsub.NewError(
			pubsub.ErrInvalidConfiguration,
			pubsub.ErrorCodeConfiguration,
			"invalid RabbitMQ configuration",
			string(pubsub.RabbitMQBroker),
			false,
		)
	}

	// Create a Config struct from the map
	cfg := &Config{}

	// Extract URI
	if uri, ok := rabbitConfig["uri"].(string); ok {
		cfg.URI = uri
	}

	// Extract other fields...
	// (In a real implementation, you would extract all fields)

	// Validate the config
	return ValidateConfig(cfg)
}

// ValidateSubscriberConfig validates the subscriber configuration
func ValidateSubscriberConfig(config map[string]interface{}) error {
	// Similar to ValidatePublisherConfig
	return ValidatePublisherConfig(config)
}
