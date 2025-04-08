package rabbitmq

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/MitulShah1/gopubsub/internal/util/logger"
)

// Config contains RabbitMQ-specific configuration
type Config struct {
	// Connection URI (e.g., "amqp://guest:guest@localhost:5672/")
	URI string

	// ConnectionTimeout for establishing connection
	ConnectionTimeout time.Duration

	// Exchange defaults to be used if not specified
	DefaultExchangeType       string
	DefaultExchangeDurable    bool
	DefaultExchangeAutoDelete bool

	// For security settings (SSL, etc.)
	TLSConfig *amqp.Config

	// Reconnect settings
	ReconnectDelay time.Duration
	MaxReconnect   int

	// Logging configuration
	LogLevel logger.Level
}
