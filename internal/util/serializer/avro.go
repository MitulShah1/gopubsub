package serializer

import (
	"time"

	"github.com/MitulShah1/gopubsub/pkg/pubsub"
	"github.com/linkedin/goavro/v2"
)

// AvroMessageSchema is the Avro schema for messages
const AvroMessageSchema = `{
  "type": "record",
  "name": "Message",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "payload", "type": "bytes"},
    {"name": "attributes", "type": {"type": "map", "values": "string"}},
    {"name": "publish_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "version", "type": "string"},
    {"name": "metadata", "type": {"type": "map", "values": "string"}}
  ]
}`

// AvroSerializer implements Avro serialization
type AvroSerializer struct {
	codec *goavro.Codec
}

// NewAvroSerializer creates a new Avro serializer
func NewAvroSerializer() (*AvroSerializer, error) {
	codec, err := goavro.NewCodec(AvroMessageSchema)
	if err != nil {
		return nil, err
	}
	return &AvroSerializer{codec: codec}, nil
}

// Serialize converts a message to Avro
func (s *AvroSerializer) Serialize(msg *pubsub.Message) ([]byte, error) {
	// Convert the message to a map that matches the Avro schema
	avroMsg := map[string]interface{}{
		"id":           msg.ID,
		"payload":      msg.Payload,
		"attributes":   msg.Attributes,
		"publish_time": time.Now().UnixMilli(),
		"version":      "1.0",
		"metadata":     make(map[string]string),
	}

	// Convert to Avro binary format
	return s.codec.BinaryFromNative(nil, avroMsg)
}

// Deserialize converts Avro to a message
func (s *AvroSerializer) Deserialize(data []byte) (*pubsub.Message, error) {
	// Convert from Avro binary format to native Go types
	native, _, err := s.codec.NativeFromBinary(data)
	if err != nil {
		return nil, err
	}

	// Convert the native map to a Message
	avroMsg, ok := native.(map[string]interface{})
	if !ok {
		return nil, err
	}

	// Extract fields from the Avro message
	id, _ := avroMsg["id"].(string)
	payload, _ := avroMsg["payload"].([]byte)
	attributes, _ := avroMsg["attributes"].(map[string]interface{})
	publishTimeMillis, _ := avroMsg["publish_time"].(int64)

	// Convert attributes from map[interface{}]interface{} to map[string]string
	attrs := make(map[string]string)
	for k, v := range attributes {
		if str, ok := v.(string); ok {
			attrs[k] = str
		}
	}

	// Create the message
	msg := &pubsub.Message{
		ID:          id,
		Payload:     payload,
		Attributes:  attrs,
		PublishTime: time.UnixMilli(publishTimeMillis),
	}

	return msg, nil
}
