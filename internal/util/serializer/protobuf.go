package serializer

import (
	"github.com/MitulShah1/gopubsub/pkg/pubsub"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ProtobufSerializer implements Protocol Buffers serialization
type ProtobufSerializer struct{}

// NewProtobufSerializer creates a new Protobuf serializer
func NewProtobufSerializer() *ProtobufSerializer {
	return &ProtobufSerializer{}
}

// Serialize converts a message to Protobuf
func (s *ProtobufSerializer) Serialize(msg *pubsub.Message) ([]byte, error) {
	pbMsg := &PubSubMessage{
		Id:          msg.ID,
		Payload:     msg.Payload,
		Attributes:  msg.Attributes,
		PublishTime: timestamppb.Now(),
		Version:     "1.0",
		Metadata:    make(map[string]string),
	}

	return proto.Marshal(pbMsg)
}

// Deserialize converts Protobuf to a message
func (s *ProtobufSerializer) Deserialize(data []byte) (*pubsub.Message, error) {
	var pbMsg PubSubMessage
	if err := proto.Unmarshal(data, &pbMsg); err != nil {
		return nil, err
	}

	msg := &pubsub.Message{
		ID:          pbMsg.Id,
		Payload:     pbMsg.Payload,
		Attributes:  pbMsg.Attributes,
		PublishTime: pbMsg.PublishTime.AsTime(),
	}

	return msg, nil
}
