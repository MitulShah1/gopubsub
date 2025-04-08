package serializer

import (
	"encoding/json"
	"time"

	"github.com/MitulShah1/gopubsub/pkg/pubsub"
)

// MessageWrapper wraps a message with metadata
type MessageWrapper struct {
	ID          string                 `json:"id"`
	Payload     []byte                 `json:"payload"`
	Attributes  map[string]string      `json:"attributes"`
	PublishTime time.Time              `json:"publish_time"`
	Version     string                 `json:"version"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// Serializer defines the interface for message serialization
type Serializer interface {
	Serialize(msg *pubsub.Message) ([]byte, error)
	Deserialize(data []byte) (*pubsub.Message, error)
}

// JSONSerializer implements JSON serialization
type JSONSerializer struct{}

// NewJSONSerializer creates a new JSON serializer
func NewJSONSerializer() *JSONSerializer {
	return &JSONSerializer{}
}

// Serialize converts a message to JSON
func (s *JSONSerializer) Serialize(msg *pubsub.Message) ([]byte, error) {
	wrapper := MessageWrapper{
		ID:          msg.ID,
		Payload:     msg.Payload,
		Attributes:  msg.Attributes,
		PublishTime: time.Now(),
		Version:     "1.0",
		Metadata:    make(map[string]interface{}),
	}

	return json.Marshal(wrapper)
}

// Deserialize converts JSON to a message
func (s *JSONSerializer) Deserialize(data []byte) (*pubsub.Message, error) {
	var wrapper MessageWrapper
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return nil, err
	}

	msg := &pubsub.Message{
		ID:          wrapper.ID,
		Payload:     wrapper.Payload,
		Attributes:  wrapper.Attributes,
		PublishTime: wrapper.PublishTime,
	}

	return msg, nil
}
