package rabbitmq

import (
	"fmt"
	"time"

	"github.com/MitulShah1/gopubsub/internal/util/logger"
	"github.com/MitulShah1/gopubsub/pkg/pubsub"
)

// BrokerType is the type identifier for RabbitMQ
const BrokerType = "rabbitmq"

func init() {
	// Register publisher factory
	pubsub.RegisterPublisherFactory(BrokerType, func(config map[string]interface{}) (pubsub.Publisher, error) {
		cfg, err := extractConfig(config)
		if err != nil {
			return nil, fmt.Errorf("failed to extract config: %w", err)
		}
		return NewPublisher(cfg)
	})

	// Register subscriber factory
	pubsub.RegisterSubscriberFactory(BrokerType, func(config map[string]interface{}) (pubsub.Subscriber, error) {
		cfg, err := extractConfig(config)
		if err != nil {
			return nil, fmt.Errorf("failed to extract config: %w", err)
		}
		return NewSubscriber(cfg)
	})

	// Register client factory
	pubsub.RegisterClientFactory(BrokerType, func(config map[string]interface{}) (pubsub.Client, error) {
		return pubsub.NewClient(BrokerType, config)
	})
}

// extractConfig extracts RabbitMQ configuration from the generic config map
func extractConfig(config map[string]interface{}) (*Config, error) {
	rabbitmqConfig, ok := config["rabbitmq"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid rabbitmq configuration")
	}

	cfg := &Config{}

	// Extract URI
	if uri, ok := rabbitmqConfig["uri"].(string); ok {
		cfg.URI = uri
	} else {
		return nil, fmt.Errorf("missing or invalid uri in configuration")
	}

	// Extract connection timeout
	if timeout, ok := rabbitmqConfig["connection_timeout"].(string); ok {
		duration, err := time.ParseDuration(timeout)
		if err != nil {
			return nil, fmt.Errorf("invalid connection_timeout: %w", err)
		}
		cfg.ConnectionTimeout = duration
	}

	// Extract reconnect delay
	if delay, ok := rabbitmqConfig["reconnect_delay"].(string); ok {
		duration, err := time.ParseDuration(delay)
		if err != nil {
			return nil, fmt.Errorf("invalid reconnect_delay: %w", err)
		}
		cfg.ReconnectDelay = duration
	}

	// Extract max reconnect attempts
	if maxReconnect, ok := rabbitmqConfig["max_reconnect"].(int); ok {
		cfg.MaxReconnect = maxReconnect
	}

	// Extract default exchange type
	if exchangeType, ok := rabbitmqConfig["default_exchange_type"].(string); ok {
		cfg.DefaultExchangeType = exchangeType
	}

	// Extract log level
	if logLevel, ok := rabbitmqConfig["log_level"].(int); ok {
		cfg.LogLevel = logger.Level(logLevel)
	}

	return cfg, nil
}
