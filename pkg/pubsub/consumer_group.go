package pubsub

import (
	"context"
	"fmt"
	"sync"
)

// ConsumerGroup manages multiple subscribers for the same topic
type ConsumerGroup struct {
	// Topic is the topic this consumer group is subscribed to
	Topic string

	// GroupID is the unique identifier for this consumer group
	GroupID string

	// Subscribers is a map of subscriber IDs to their handlers
	Subscribers map[string]MessageHandler

	// BrokerType is the type of broker this consumer group is using
	BrokerType BrokerType

	// BrokerConfig is the configuration for the broker
	BrokerConfig BrokerConfig

	// ClientConfig is the client configuration
	ClientConfig *ClientConfig

	// Active indicates whether the consumer group is active
	Active bool

	// Mutex for thread safety
	mutex sync.RWMutex
}

// NewConsumerGroup creates a new consumer group
func NewConsumerGroup(topic, groupID string, brokerType BrokerType, config *ClientConfig) (*ConsumerGroup, error) {
	if config == nil {
		var err error
		config, err = DefaultClientConfig(brokerType)
		if err != nil {
			return nil, err
		}
	}

	return &ConsumerGroup{
		Topic:        topic,
		GroupID:      groupID,
		Subscribers:  make(map[string]MessageHandler),
		BrokerType:   brokerType,
		BrokerConfig: config.BrokerConfig,
		ClientConfig: config,
		Active:       false,
	}, nil
}

// AddSubscriber adds a new subscriber to the consumer group
func (cg *ConsumerGroup) AddSubscriber(subscriberID string, handler MessageHandler) error {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	if cg.Subscribers[subscriberID] != nil {
		return fmt.Errorf("subscriber with ID %s already exists", subscriberID)
	}

	cg.Subscribers[subscriberID] = handler

	// If the consumer group is already active, we need to restart it
	// to include the new subscriber
	if cg.Active {
		// This is a simplified approach - in a real implementation,
		// you would need to handle the restart more gracefully
		cg.Active = false
		if err := cg.Start(context.Background()); err != nil {
			return fmt.Errorf("failed to restart consumer group: %w", err)
		}
	}

	return nil
}

// RemoveSubscriber removes a subscriber from the consumer group
func (cg *ConsumerGroup) RemoveSubscriber(subscriberID string) error {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	if cg.Subscribers[subscriberID] == nil {
		return fmt.Errorf("subscriber with ID %s does not exist", subscriberID)
	}

	delete(cg.Subscribers, subscriberID)

	// If the consumer group is active and this was the last subscriber,
	// we should stop it
	if cg.Active && len(cg.Subscribers) == 0 {
		cg.Active = false
		// In a real implementation, you would need to handle the stop more gracefully
	}

	return nil
}

// Start starts the consumer group
func (cg *ConsumerGroup) Start(ctx context.Context) error {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	if cg.Active {
		return fmt.Errorf("consumer group is already active")
	}

	if len(cg.Subscribers) == 0 {
		return fmt.Errorf("no subscribers in consumer group")
	}

	// Create a subscriber for this consumer group
	subscriber, err := NewSubscriber(cg.BrokerType, cg.BrokerConfig)
	if err != nil {
		return fmt.Errorf("failed to create subscriber: %w", err)
	}

	// Create a handler that distributes messages to all subscribers
	groupHandler := func(ctx context.Context, msg *Message) error {
		// In a real implementation, you would need to handle errors from individual subscribers
		// and potentially implement retry logic
		var lastErr error
		for id, handler := range cg.Subscribers {
			// Create a copy of the message for each subscriber
			msgCopy := msg.Clone()

			// Add subscriber ID to the message attributes
			msgCopy.Attributes["subscriber_id"] = id

			// Process the message with the subscriber's handler
			if err := handler(ctx, msgCopy); err != nil {
				lastErr = err
				// Log the error but continue processing with other subscribers
				fmt.Printf("Error processing message with subscriber %s: %v\n", id, err)
			}
		}
		return lastErr
	}

	// Subscribe to the topic with the group handler
	if err := subscriber.Subscribe(ctx, cg.Topic, groupHandler); err != nil {
		return fmt.Errorf("failed to subscribe to topic: %w", err)
	}

	cg.Active = true
	return nil
}

// Stop stops the consumer group
func (cg *ConsumerGroup) Stop() error {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	if !cg.Active {
		return nil
	}

	// In a real implementation, you would need to properly stop the subscriber
	// and clean up resources
	cg.Active = false
	return nil
}

// GetSubscriberCount returns the number of subscribers in the consumer group
func (cg *ConsumerGroup) GetSubscriberCount() int {
	cg.mutex.RLock()
	defer cg.mutex.RUnlock()
	return len(cg.Subscribers)
}

// GetSubscriberIDs returns the IDs of all subscribers in the consumer group
func (cg *ConsumerGroup) GetSubscriberIDs() []string {
	cg.mutex.RLock()
	defer cg.mutex.RUnlock()

	ids := make([]string, 0, len(cg.Subscribers))
	for id := range cg.Subscribers {
		ids = append(ids, id)
	}
	return ids
}
