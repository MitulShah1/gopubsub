package rabbitmq

import (
	"context"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/MitulShah1/gopubsub/internal/util/logger"
	"github.com/MitulShah1/gopubsub/pkg/pubsub"
)

// Subscriber implements the pubsub.Subscriber interface for RabbitMQ
type Subscriber struct {
	config        *Config
	conn          *amqp.Connection
	channel       *amqp.Channel
	queues        map[string]bool
	channelLock   sync.Mutex
	closed        bool
	closedLock    sync.RWMutex
	consumers     map[string]context.CancelFunc
	consumersLock sync.Mutex
	log           logger.Logger
}

// NewSubscriber creates a new RabbitMQ subscriber
func NewSubscriber(config *Config) (*Subscriber, error) {
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

	subscriber := &Subscriber{
		config:    config,
		queues:    make(map[string]bool),
		consumers: make(map[string]context.CancelFunc),
		log:       logger.New(config.LogLevel).WithFields(logger.Fields{"broker": "rabbitmq"}),
	}

	// Connect to RabbitMQ
	err := subscriber.connect()
	if err != nil {
		return nil, err
	}

	return subscriber, nil
}

// connect establishes connection to RabbitMQ
func (s *Subscriber) connect() error {
	s.channelLock.Lock()
	defer s.channelLock.Unlock()

	// Create connection
	conn, err := amqp.DialConfig(s.config.URI, *s.config.TLSConfig)
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
	go s.monitorConnection(conn)

	s.conn = conn
	s.channel = channel
	return nil
}

// monitorConnection watches the connection and reconnects when necessary
func (s *Subscriber) monitorConnection(conn *amqp.Connection) {
	// Create a channel to receive connection close notifications
	connCloseChan := conn.NotifyClose(make(chan *amqp.Error, 1))

	// Block until the connection is closed
	err := <-connCloseChan

	s.closedLock.RLock()
	closed := s.closed
	s.closedLock.RUnlock()

	if closed {
		// If we're closed, don't reconnect
		return
	}

	// Log the error
	s.log.Error("RabbitMQ connection closed", logger.Fields{
		"error": err.Error(),
	})

	// Attempt to reconnect
	reconnectCount := 0
	for {
		reconnectCount++
		if reconnectCount > s.config.MaxReconnect {
			s.log.Error("Max reconnect attempts reached, giving up", logger.Fields{
				"max_attempts": s.config.MaxReconnect,
			})
			return
		}

		s.log.Info("Attempting to reconnect to RabbitMQ", logger.Fields{
			"attempt":      reconnectCount,
			"max_attempts": s.config.MaxReconnect,
		})

		// Wait before reconnecting
		time.Sleep(s.config.ReconnectDelay)

		// Attempt reconnection
		err := s.connect()
		if err == nil {
			s.log.Info("Successfully reconnected to RabbitMQ", nil)

			// Resubscribe to all queues
			s.resubscribe()
			return
		}

		s.log.Error("Failed to reconnect", logger.Fields{
			"error": err.Error(),
		})
	}
}

// resubscribe attempts to reestablish all subscriptions after reconnection
func (s *Subscriber) resubscribe() {
	// This would need to store subscription details
	// and recreate them after reconnection
	// Implementation depends on how subscriptions are tracked
}

// ensureExchange makes sure an exchange exists
func (s *Subscriber) ensureExchange(exchange string, exchangeType string) error {
	s.channelLock.Lock()
	defer s.channelLock.Unlock()

	// If no exchange type provided, use the default
	if exchangeType == "" {
		exchangeType = s.config.DefaultExchangeType
	}

	// Declare the exchange
	err := s.channel.ExchangeDeclare(
		exchange,                           // name
		exchangeType,                       // type
		s.config.DefaultExchangeDurable,    // durable
		s.config.DefaultExchangeAutoDelete, // auto-deleted
		false,                              // internal
		false,                              // no-wait
		nil,                                // arguments
	)
	if err != nil {
		return pubsub.NewError(
			err,
			pubsub.ErrorCodeSubscribe,
			"failed to declare exchange",
			string(pubsub.RabbitMQBroker),
			true,
		)
	}

	return nil
}

// ensureQueue makes sure a queue exists and is bound to the exchange
func (s *Subscriber) ensureQueue(queueName, exchange, routingKey string, options *pubsub.SubscribeOptions) (string, error) {
	s.channelLock.Lock()
	defer s.channelLock.Unlock()

	// If queue name is empty and consumer group is provided, use consumer group as queue name
	if queueName == "" && options.ConsumerGroup != "" {
		queueName = options.ConsumerGroup
	}

	// If queue name is still empty, generate a unique name
	if queueName == "" {
		var err error
		queueName, err = s.generateQueueName(exchange)
		if err != nil {
			return "", err
		}
	}

	// Set up arguments for dead letter queue if configured
	args := amqp.Table{}
	if options.DeadLetterConfig != nil && options.DeadLetterConfig.Enable && options.DeadLetterConfig.Topic != "" {
		args["x-dead-letter-exchange"] = options.DeadLetterConfig.Topic
		if options.DeadLetterConfig.MaxRetries > 0 {
			args["x-message-ttl"] = int32(5000) // 5 seconds TTL before retrying
			args["x-max-retries"] = int32(options.DeadLetterConfig.MaxRetries)
		}
	}

	// Declare the queue
	queue, err := s.channel.QueueDeclare(
		queueName,       // name
		options.Durable, // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		args,            // arguments
	)
	if err != nil {
		return "", pubsub.NewError(
			err,
			pubsub.ErrorCodeSubscribe,
			"failed to declare queue",
			string(pubsub.RabbitMQBroker),
			true,
		)
	}

	// Bind the queue to the exchange
	err = s.channel.QueueBind(
		queue.Name, // queue name
		routingKey, // routing key
		exchange,   // exchange
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		return "", pubsub.NewError(
			err,
			pubsub.ErrorCodeSubscribe,
			"failed to bind queue",
			string(pubsub.RabbitMQBroker),
			true,
		)
	}

	// Set QoS if prefetch count is specified
	if options.PrefetchCount > 0 {
		err = s.channel.Qos(
			options.PrefetchCount, // prefetch count
			0,                     // prefetch size
			false,                 // global
		)
		if err != nil {
			return "", pubsub.NewError(
				err,
				pubsub.ErrorCodeSubscribe,
				"failed to set QoS",
				string(pubsub.RabbitMQBroker),
				true,
			)
		}
	}

	return queue.Name, nil
}

// generateQueueName creates a unique queue name for the given exchange
func (s *Subscriber) generateQueueName(exchange string) (string, error) {
	// Create a temporary queue with a random name
	queue, err := s.channel.QueueDeclare(
		"",    // name - empty for auto-generated
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return "", pubsub.NewError(
			err,
			pubsub.ErrorCodeSubscribe,
			"failed to generate queue name",
			string(pubsub.RabbitMQBroker),
			true,
		)
	}

	return queue.Name, nil
}

// Subscribe consumes messages from a RabbitMQ queue
func (s *Subscriber) Subscribe(ctx context.Context, topic string, handler pubsub.MessageHandler, opts ...pubsub.SubscribeOption) error {
	// Apply options
	options := pubsub.ApplySubscribeOptions(opts...)

	// Extract RabbitMQ specific options
	var exchangeType string
	var routingKey string
	var queueName string

	// Default routing key is wildcard
	routingKey = "#"

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
		if val, ok := options.BrokerSpecificOpts["queue_name"]; ok {
			if strVal, ok := val.(string); ok {
				queueName = strVal
			}
		}
	}

	// Ensure the exchange exists
	if err := s.ensureExchange(topic, exchangeType); err != nil {
		return err
	}

	// Ensure the queue exists and is bound to the exchange
	queue, err := s.ensureQueue(queueName, topic, routingKey, options)
	if err != nil {
		return err
	}

	// Create a context for this consumer
	consumerCtx, cancel := context.WithCancel(ctx)

	// Store the cancel function so we can cancel this consumer later
	s.consumersLock.Lock()
	s.consumers[queue] = cancel
	s.consumersLock.Unlock()

	s.log.Info("Starting consumer", logger.Fields{
		"queue":       queue,
		"topic":       topic,
		"exchange":    topic,
		"routing_key": routingKey,
	})

	// Start consuming in a separate goroutine
	go func() {
		defer cancel()

		for {
			select {
			case <-consumerCtx.Done():
				s.log.Info("Consumer context cancelled", logger.Fields{
					"queue": queue,
				})
				return
			default:
				// Create a channel for consuming messages
				msgs, err := s.channel.Consume(
					queue, // queue
					"",    // consumer
					false, // auto-ack
					false, // exclusive
					false, // no-local
					false, // no-wait
					nil,   // args
				)
				if err != nil {
					s.log.Error("Failed to start consuming", logger.Fields{
						"queue": queue,
						"error": err.Error(),
					})
					return
				}

				// Process messages
				for msg := range msgs {
					// Convert AMQP message to pubsub.Message
					pubsubMsg := &pubsub.Message{
						ID:              msg.MessageId,
						Payload:         msg.Body,
						Attributes:      make(map[string]string),
						PublishTime:     time.Now(),
						DeliveryAttempt: 1,
						OriginalMessage: msg,
					}

					// Copy headers to attributes
					for k, v := range msg.Headers {
						if strVal, ok := v.(string); ok {
							pubsubMsg.Attributes[k] = strVal
						}
					}

					// Process the message
					err := handler(ctx, pubsubMsg)
					if err != nil {
						s.log.Error("Message handler failed", logger.Fields{
							"error": err.Error(),
						})
						if options.AutoAck {
							if nackErr := s.Nack(ctx, pubsubMsg); nackErr != nil {
								s.log.Error("Failed to nack message", logger.Fields{
									"error": nackErr.Error(),
								})
							}
						}
						continue
					}

					// Auto-acknowledge if enabled
					if options.AutoAck {
						if ackErr := s.Ack(ctx, pubsubMsg); ackErr != nil {
							s.log.Error("Failed to ack message", logger.Fields{
								"error": ackErr.Error(),
							})
						}
					}
				}

				// If we get here, the channel was closed
				s.log.Warn("Consumer channel closed, attempting to reconnect", logger.Fields{
					"queue": queue,
				})
				time.Sleep(s.config.ReconnectDelay)
			}
		}
	}()

	return nil
}

// Ack acknowledges a message
func (s *Subscriber) Ack(ctx context.Context, msg *pubsub.Message) error {
	if delivery, ok := msg.OriginalMessage.(amqp.Delivery); ok {
		err := delivery.Ack(false)
		if err != nil {
			return pubsub.NewError(
				err,
				pubsub.ErrorCodeAcknowledge,
				"failed to acknowledge message",
				string(pubsub.RabbitMQBroker),
				true,
			)
		}
		return nil
	}
	return pubsub.NewError(
		pubsub.ErrInvalidMessage,
		pubsub.ErrorCodeInvalidMessage,
		"message does not contain valid RabbitMQ delivery",
		string(pubsub.RabbitMQBroker),
		false,
	)
}

// Nack indicates that the message processing failed and should be handled according to the broker's configuration
func (s *Subscriber) Nack(ctx context.Context, msg *pubsub.Message, opts ...pubsub.NackOption) error {
	if delivery, ok := msg.OriginalMessage.(amqp.Delivery); ok {
		// Create default options
		options := &pubsub.NackOptions{
			Requeue:            true,
			RequeueDelay:       0,
			AdditionalInfo:     "",
			BrokerSpecificOpts: make(map[string]interface{}),
		}

		// Apply provided options
		for _, opt := range opts {
			opt(options)
		}

		// Get requeue option from broker-specific options if available
		requeue := options.Requeue
		if val, ok := options.BrokerSpecificOpts["requeue"]; ok {
			if boolVal, ok := val.(bool); ok {
				requeue = boolVal
			}
		}

		err := delivery.Nack(false, requeue)
		if err != nil {
			return pubsub.NewError(
				err,
				pubsub.ErrorCodeAcknowledge,
				"failed to negative acknowledge message",
				string(pubsub.RabbitMQBroker),
				true,
			)
		}
		return nil
	}
	return pubsub.NewError(
		pubsub.ErrInvalidMessage,
		pubsub.ErrorCodeInvalidMessage,
		"message does not contain valid RabbitMQ delivery",
		string(pubsub.RabbitMQBroker),
		false,
	)
}

// Close closes the subscriber and cleans up resources
func (s *Subscriber) Close() error {
	s.closedLock.Lock()
	defer s.closedLock.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	// Cancel all consumers
	s.consumersLock.Lock()
	for _, cancel := range s.consumers {
		cancel()
	}
	s.consumers = make(map[string]context.CancelFunc)
	s.consumersLock.Unlock()

	// Close channel and connection
	s.channelLock.Lock()
	defer s.channelLock.Unlock()

	if s.channel != nil {
		if err := s.channel.Close(); err != nil {
			return pubsub.NewError(
				err,
				pubsub.ErrorCodeConnection,
				"failed to close channel",
				string(pubsub.RabbitMQBroker),
				false,
			)
		}
	}

	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			return pubsub.NewError(
				err,
				pubsub.ErrorCodeConnection,
				"failed to close connection",
				string(pubsub.RabbitMQBroker),
				false,
			)
		}
	}

	return nil
}
