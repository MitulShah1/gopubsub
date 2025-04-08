package pubsub

import (
	"fmt"
	"time"
)

// Message represents a message to be published or received
type Message struct {
	// ID is a unique identifier for the message
	ID string

	// Payload contains the message data
	Payload []byte

	// Attributes are key-value pairs that can be used to add metadata to the message
	Attributes map[string]string

	// PublishTime is when the message was published
	PublishTime time.Time

	// DeliveryAttempt represents the number of delivery attempts
	DeliveryAttempt int

	// SequenceNumber is a broker-assigned sequence identifier (if available)
	SequenceNumber int64

	// OriginalMessage contains the raw message from the specific broker
	// implementation, allowing for accessing broker-specific features
	OriginalMessage interface{}
}

// NewMessage creates a new message with the specified payload
func NewMessage(payload []byte) *Message {
	return &Message{
		ID:              "",
		Payload:         payload,
		Attributes:      make(map[string]string),
		PublishTime:     time.Now(),
		DeliveryAttempt: 0,
	}
}

// String returns a string representation of the message
func (m *Message) String() string {
	return fmt.Sprintf("Message{ID: %s, PayloadSize: %d bytes, Attributes: %d, DeliveryAttempt: %d}",
		m.ID, len(m.Payload), len(m.Attributes), m.DeliveryAttempt)
}

// Clone creates a deep copy of the message
func (m *Message) Clone() *Message {
	if m == nil {
		return nil
	}

	// Create a new message with the same basic fields
	clone := &Message{
		ID:              m.ID,
		Payload:         make([]byte, len(m.Payload)),
		Attributes:      make(map[string]string, len(m.Attributes)),
		PublishTime:     m.PublishTime,
		DeliveryAttempt: m.DeliveryAttempt,
		SequenceNumber:  m.SequenceNumber,
		OriginalMessage: m.OriginalMessage,
	}

	// Copy the data
	copy(clone.Payload, m.Payload)

	// Copy the attributes
	if m.Attributes != nil {
		for k, v := range m.Attributes {
			clone.Attributes[k] = v
		}
	}

	return clone
}

// SetAttribute sets an attribute on the message
func (m *Message) SetAttribute(key, value string) {
	if m.Attributes == nil {
		m.Attributes = make(map[string]string)
	}
	m.Attributes[key] = value
}

// GetAttribute gets an attribute from the message
func (m *Message) GetAttribute(key string) (string, bool) {
	if m.Attributes == nil {
		return "", false
	}
	value, exists := m.Attributes[key]
	return value, exists
}

// ContentType gets the content type attribute if present
func (m *Message) ContentType() string {
	contentType, exists := m.GetAttribute("content-type")
	if !exists {
		return ""
	}
	return contentType
}

// SetContentType sets the content type attribute
func (m *Message) SetContentType(contentType string) {
	m.SetAttribute("content-type", contentType)
}

// Size returns the total size of the message in bytes (payload + attributes)
func (m *Message) Size() int {
	size := len(m.Payload)

	// Add rough estimate of attributes size
	for k, v := range m.Attributes {
		size += len(k) + len(v)
	}

	return size
}
