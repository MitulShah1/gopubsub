package rabbitmq

import (
	"context"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/MitulShah1/gopubsub/internal/util/logger"
	"github.com/MitulShah1/gopubsub/pkg/pubsub"
)

// Publisher implements the pubsub.Publisher interface for RabbitMQ
type Publisher struct {
	config      *Config
	conn        *amqp.Connection
	channel     *amqp.Channel
	exchanges   map[string]bool
	channelLock sync.Mutex
	closed      bool
	closedLock  sync.RWMutex
	log         logger.Logger
}

// NewPublisher creates a new RabbitMQ publisher
func NewPublisher(config *Config) (*Publisher, error) {
	if config.URI == "" {
		return nil, pubsub.NewError(
			pubsub.ErrInvalidConfiguration,
			pubsub.ErrorCodeConfiguration,
			"RabbitMQ URI is required",
			string(pubsub.RabbitMQBroker),
			false,
		)
	}

	// Set defaults if not provided
	if config.ConnectionTimeout == 0 {
		config.ConnectionTimeout = 30 * time.Second
	}
	if config.DefaultExchangeType == "" {
		config.DefaultExchangeType = "topic"
	}
	if config.ReconnectDelay == 0 {
		config.ReconnectDelay = 5 * time.Second
	}
	if config.MaxReconnect == 0 {
		config.MaxReconnect = 10
	}
	if config.LogLevel == 0 {
		config.LogLevel = logger.InfoLevel
	}

	publisher := &Publisher{
		config:    config,
		exchanges: make(map[string]bool),
		log:       logger.New(config.LogLevel).WithFields(logger.Fields{"broker": "rabbitmq"}),
	}

	// Connect to RabbitMQ
	err := publisher.connect()
	if err != nil {
		return nil, err
	}

	return publisher, nil
}

// connect establishes connection to RabbitMQ
func (p *Publisher) connect() error {
	p.channelLock.Lock()
	defer p.channelLock.Unlock()

	// Create connection
	conn, err := amqp.DialConfig(p.config.URI, *p.config.TLSConfig)
	if err != nil {
		return pubsub.NewError(
			pubsub.ErrConnectionFailed,
			pubsub.ErrorCodeConnection,
			"failed to connect to RabbitMQ",
			string(pubsub.RabbitMQBroker),
			true,
		)
	}

	// Create channel
	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return pubsub.NewError(
			pubsub.ErrConnectionFailed,
			pubsub.ErrorCodeConnection,
			"failed to create channel",
			string(pubsub.RabbitMQBroker),
			true,
		)
	}

	// Monitor connection and reconnect if needed
	go p.monitorConnection(conn)

	p.conn = conn
	p.channel = channel
	return nil
}

// monitorConnection watches the connection and reconnects when necessary
func (p *Publisher) monitorConnection(conn *amqp.Connection) {
	// Create a channel to receive connection close notifications
	connCloseChan := conn.NotifyClose(make(chan *amqp.Error, 1))

	// Block until the connection is closed
	err := <-connCloseChan

	p.closedLock.RLock()
	closed := p.closed
	p.closedLock.RUnlock()

	if closed {
		// If we're closed, don't reconnect
		return
	}

	// Log the error
	p.log.Error("RabbitMQ connection closed", logger.Fields{
		"error": err.Error(),
	})

	// Attempt to reconnect
	reconnectCount := 0
	for {
		reconnectCount++
		if reconnectCount > p.config.MaxReconnect {
			p.log.Error("Max reconnect attempts reached, giving up", logger.Fields{
				"max_attempts": p.config.MaxReconnect,
			})
			return
		}

		p.log.Info("Attempting to reconnect to RabbitMQ", logger.Fields{
			"attempt":      reconnectCount,
			"max_attempts": p.config.MaxReconnect,
		})

		// Wait before reconnecting
		time.Sleep(p.config.ReconnectDelay)

		// Attempt reconnection
		err := p.connect()
		if err == nil {
			p.log.Info("Successfully reconnected to RabbitMQ", nil)
			return
		}

		p.log.Error("Failed to reconnect", logger.Fields{
			"error": err.Error(),
		})
	}
}

// ensureExchange makes sure an exchange exists
func (p *Publisher) ensureExchange(exchange string, exchangeType string) error {
	p.channelLock.Lock()
	defer p.channelLock.Unlock()

	// If we've already declared this exchange, don't declare it again
	if _, exists := p.exchanges[exchange]; exists {
		return nil
	}

	// If no exchange type provided, use the default
	if exchangeType == "" {
		exchangeType = p.config.DefaultExchangeType
	}

	// Declare the exchange
	err := p.channel.ExchangeDeclare(
		exchange,                           // name
		exchangeType,                       // type
		p.config.DefaultExchangeDurable,    // durable
		p.config.DefaultExchangeAutoDelete, // auto-deleted
		false,                              // internal
		false,                              // no-wait
		nil,                                // arguments
	)
	if err != nil {
		return pubsub.NewError(
			err,
			pubsub.ErrorCodePublish,
			"failed to declare exchange",
			string(pubsub.RabbitMQBroker),
			true,
		)
	}

	// Remember that we've declared this exchange
	p.exchanges[exchange] = true
	return nil
}

// Publish sends a message to a RabbitMQ exchange
func (p *Publisher) Publish(ctx context.Context, topic string, msg *pubsub.Message, opts ...pubsub.PublishOption) error {
	// Apply options
	options := pubsub.ApplyPublishOptions(opts...)

	// Extract RabbitMQ specific options
	var exchangeType string
	var routingKey string

	// Default routing key is the topic
	routingKey = topic

	// Check for broker-specific options
	if options.BrokerSpecificOpts != nil {
		if val, ok := options.BrokerSpecificOpts["exchange_type"]; ok {
			if strVal, ok := val.(string); ok {
				exchangeType = strVal
			}
		}
		if val, ok := options.BrokerSpecificOpts["routing_key"]; ok {
			if strVal, ok := val.(string); ok {
				routingKey = strVal
			}
		}
	}

	p.log.Debug("Publishing message", logger.Fields{
		"topic":       topic,
		"message_id":  msg.ID,
		"routing_key": routingKey,
		"exchange":    topic,
	})

	// Ensure the exchange exists
	if err := p.ensureExchange(topic, exchangeType); err != nil {
		p.log.Error("Failed to ensure exchange", logger.Fields{
			"topic": topic,
			"error": err.Error(),
		})
		return err
	}

	// Create AMQP message
	amqpMsg := amqp.Publishing{
		ContentType:  msg.ContentType(),
		Body:         msg.Payload,
		DeliveryMode: amqp.Transient,
		Timestamp:    time.Now(),
		MessageId:    msg.ID,
		Headers:      make(amqp.Table),
	}

	// Set persistence
	if options.Persistent {
		amqpMsg.DeliveryMode = amqp.Persistent
	}

	// Set priority
	if options.Priority > 0 {
		amqpMsg.Priority = uint8(options.Priority)
	}

	// Copy attributes to headers
	for k, v := range msg.Attributes {
		amqpMsg.Headers[k] = v
	}

	// Create a context with timeout if specified
	if options.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, options.Timeout)
		defer cancel()
	}

	// Wait for context cancellation or publish
	publishCh := make(chan error, 1)
	go func() {
		p.channelLock.Lock()
		defer p.channelLock.Unlock()

		// Publish the message
		err := p.channel.Publish(
			topic,      // exchange
			routingKey, // routing key
			false,      // mandatory
			false,      // immediate
			amqpMsg,    // message
		)

		publishCh <- err
	}()

	select {
	case err := <-publishCh:
		if err != nil {
			p.log.Error("Failed to publish message", logger.Fields{
				"topic":      topic,
				"message_id": msg.ID,
				"error":      err.Error(),
			})
			return pubsub.NewError(
				err,
				pubsub.ErrorCodePublish,
				"failed to publish message",
				string(pubsub.RabbitMQBroker),
				true,
			)
		}
		p.log.Debug("Successfully published message", logger.Fields{
			"topic":      topic,
			"message_id": msg.ID,
		})
		return nil
	case <-ctx.Done():
		p.log.Error("Publish operation timed out", logger.Fields{
			"topic":      topic,
			"message_id": msg.ID,
			"error":      ctx.Err().Error(),
		})
		return pubsub.NewError(
			ctx.Err(),
			pubsub.ErrorCodeTimeout,
			"publish operation timed out",
			string(pubsub.RabbitMQBroker),
			true,
		)
	}
}

// PublishBatch sends multiple messages to a RabbitMQ exchange
func (p *Publisher) PublishBatch(ctx context.Context, topic string, msgs []*pubsub.Message, opts ...pubsub.PublishOption) error {
	p.log.Debug("Publishing batch of messages", logger.Fields{
		"topic":         topic,
		"message_count": len(msgs),
	})

	for _, msg := range msgs {
		if err := p.Publish(ctx, topic, msg, opts...); err != nil {
			p.log.Error("Failed to publish message in batch", logger.Fields{
				"topic":      topic,
				"message_id": msg.ID,
				"error":      err.Error(),
			})
			return err
		}
	}

	p.log.Debug("Successfully published batch of messages", logger.Fields{
		"topic":         topic,
		"message_count": len(msgs),
	})
	return nil
}

// Close closes the RabbitMQ connection
func (p *Publisher) Close() error {
	p.closedLock.Lock()
	defer p.closedLock.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true
	p.log.Info("Closing RabbitMQ connection", nil)

	p.channelLock.Lock()
	defer p.channelLock.Unlock()

	var err error
	if p.channel != nil {
		err = p.channel.Close()
		p.channel = nil
	}

	if p.conn != nil {
		if connErr := p.conn.Close(); connErr != nil && err == nil {
			err = connErr
		}
		p.conn = nil
	}

	if err != nil {
		p.log.Error("Failed to close RabbitMQ connection", logger.Fields{
			"error": err.Error(),
		})
		return pubsub.NewError(
			err,
			pubsub.ErrorCodeConnection,
			"failed to close RabbitMQ connection",
			string(pubsub.RabbitMQBroker),
			false,
		)
	}

	p.log.Info("Successfully closed RabbitMQ connection", nil)
	return nil
}
