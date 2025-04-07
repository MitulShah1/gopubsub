package pubsub

import (
	"errors"
	"fmt"
)

// Standard error types for the pubsub package
var (
	// ErrConnectionFailed occurs when connection to the broker fails
	ErrConnectionFailed = errors.New("connection to message broker failed")

	// ErrPublishFailed occurs when a message cannot be published
	ErrPublishFailed = errors.New("failed to publish message")

	// ErrSubscribeFailed occurs when subscription fails
	ErrSubscribeFailed = errors.New("failed to subscribe to topic")

	// ErrAckFailed occurs when acknowledging a message fails
	ErrAckFailed = errors.New("failed to acknowledge message")

	// ErrNackFailed occurs when negative acknowledging a message fails
	ErrNackFailed = errors.New("failed to negative acknowledge message")

	// ErrInvalidConfiguration occurs when configuration is invalid
	ErrInvalidConfiguration = errors.New("invalid configuration")

	// ErrTimeout occurs when an operation times out
	ErrTimeout = errors.New("operation timed out")

	// ErrNotSupported occurs when a feature is not supported by a specific broker
	ErrNotSupported = errors.New("feature not supported by this broker")

	// ErrUnsupportedBroker occurs when the requested broker type is not supported
	ErrUnsupportedBroker = errors.New("unsupported broker type")

	// ErrTopicNotFound occurs when the requested topic does not exist
	ErrTopicNotFound = errors.New("topic not found")

	// ErrSubscriptionExists occurs when attempting to create a subscription that already exists
	ErrSubscriptionExists = errors.New("subscription already exists")

	// ErrConsumerGroupNotFound occurs when the requested consumer group does not exist
	ErrConsumerGroupNotFound = errors.New("consumer group not found")

	// ErrInvalidMessage occurs when a message is invalid
	ErrInvalidMessage = errors.New("invalid message")

	// ErrAlreadyClosed occurs when operating on a closed client
	ErrAlreadyClosed = errors.New("client already closed")

	// ErrPermissionDenied occurs when the client lacks permission for an operation
	ErrPermissionDenied = errors.New("permission denied")
)

// Error represents an error within the pubsub package
type Error struct {
	// Original is the underlying error
	Original error

	// Code is a specific error code
	Code ErrorCode

	// Message is a human-readable description
	Message string

	// BrokerType indicates which broker generated the error
	BrokerType string

	// Retryable indicates if the operation can be retried
	Retryable bool
}

// ErrorCode is a typed error code
type ErrorCode int

const (
	// ErrorCodeUnknown represents an unknown error
	ErrorCodeUnknown ErrorCode = iota

	// ErrorCodeConnection represents connection errors
	ErrorCodeConnection

	// ErrorCodeConfiguration represents configuration errors
	ErrorCodeConfiguration

	// ErrorCodeAuthentication represents authentication errors
	ErrorCodeAuthentication

	// ErrorCodeAuthorization represents authorization errors
	ErrorCodeAuthorization

	// ErrorCodePublish represents publishing errors
	ErrorCodePublish

	// ErrorCodeSubscribe represents subscription errors
	ErrorCodeSubscribe

	// ErrorCodeAcknowledge represents acknowledgment errors
	ErrorCodeAcknowledge

	// ErrorCodeTimeout represents timeout errors
	ErrorCodeTimeout

	// ErrorCodeNotFound represents resource not found errors
	ErrorCodeNotFound

	// ErrorCodeAlreadyExists represents resource already exists errors
	ErrorCodeAlreadyExists

	// ErrorCodeInvalidMessage represents invalid message errors
	ErrorCodeInvalidMessage

	// ErrorCodeResourceExhaustion represents resource exhaustion errors
	ErrorCodeResourceExhaustion
)

// Error implements the error interface
func (e *Error) Error() string {
	if e.Original != nil {
		return fmt.Sprintf("%s: %s (broker: %s, code: %d, retryable: %t)",
			e.Message, e.Original.Error(), e.BrokerType, e.Code, e.Retryable)
	}
	return fmt.Sprintf("%s (broker: %s, code: %d, retryable: %t)",
		e.Message, e.BrokerType, e.Code, e.Retryable)
}

// Unwrap returns the underlying error
func (e *Error) Unwrap() error {
	return e.Original
}

// NewError creates a new Error
func NewError(original error, code ErrorCode, message string, brokerType string, retryable bool) *Error {
	return &Error{
		Original:   original,
		Code:       code,
		Message:    message,
		BrokerType: brokerType,
		Retryable:  retryable,
	}
}

// IsRetryable returns whether an error can be retried
func IsRetryable(err error) bool {
	var pubsubErr *Error
	if errors.As(err, &pubsubErr) {
		return pubsubErr.Retryable
	}
	return false
}

// GetErrorCode extracts the error code from an error
func GetErrorCode(err error) ErrorCode {
	var pubsubErr *Error
	if errors.As(err, &pubsubErr) {
		return pubsubErr.Code
	}
	return ErrorCodeUnknown
}

// IsErrorCode checks if an error has a specific error code
func IsErrorCode(err error, code ErrorCode) bool {
	return GetErrorCode(err) == code
}

// IsConnectionError checks if an error is a connection error
func IsConnectionError(err error) bool {
	return IsErrorCode(err, ErrorCodeConnection) || errors.Is(err, ErrConnectionFailed)
}

// IsTimeoutError checks if an error is a timeout error
func IsTimeoutError(err error) bool {
	return IsErrorCode(err, ErrorCodeTimeout) || errors.Is(err, ErrTimeout)
}

// IsConfigurationError checks if an error is a configuration error
func IsConfigurationError(err error) bool {
	return IsErrorCode(err, ErrorCodeConfiguration) || errors.Is(err, ErrInvalidConfiguration)
}

// IsPublishError checks if an error is a publish error
func IsPublishError(err error) bool {
	return IsErrorCode(err, ErrorCodePublish) || errors.Is(err, ErrPublishFailed)
}

// IsSubscribeError checks if an error is a subscribe error
func IsSubscribeError(err error) bool {
	return IsErrorCode(err, ErrorCodeSubscribe) || errors.Is(err, ErrSubscribeFailed)
}

// IsAcknowledgeError checks if an error is an acknowledge error
func IsAcknowledgeError(err error) bool {
	return IsErrorCode(err, ErrorCodeAcknowledge) || errors.Is(err, ErrAckFailed) || errors.Is(err, ErrNackFailed)
}

// IsNotFoundError checks if an error is a not found error
func IsNotFoundError(err error) bool {
	return IsErrorCode(err, ErrorCodeNotFound) || errors.Is(err, ErrTopicNotFound) || errors.Is(err, ErrConsumerGroupNotFound)
}

// IsPermissionError checks if an error is a permission error
func IsPermissionError(err error) bool {
	var permissionErrors = []ErrorCode{
		ErrorCodeAuthentication,
		ErrorCodeAuthorization,
	}

	for _, code := range permissionErrors {
		if IsErrorCode(err, code) {
			return true
		}
	}

	return errors.Is(err, ErrPermissionDenied)
}

// BrokerType defines the type of message broker
type BrokerType string

const (
	// KafkaBroker represents Apache Kafka
	KafkaBroker BrokerType = "kafka"

	// NATSBroker represents NATS messaging system
	NATSBroker BrokerType = "nats"

	// RabbitMQBroker represents RabbitMQ
	RabbitMQBroker BrokerType = "rabbitmq"

	// GooglePubSub represents Google Cloud Pub/Sub
	GooglePubSub BrokerType = "google"

	// AzureEventHub represents Azure Event Hubs
	AzureEventHub BrokerType = "eventhub"

	// RedisPubSub represents Redis Pub/Sub
	RedisPubSub BrokerType = "redis"

	// AmazonSNS represents Amazon SNS
	AmazonSNS BrokerType = "sns"

	// AmazonSQS represents Amazon SQS
	AmazonSQS BrokerType = "sqs"

	// InMemoryBroker represents an in-memory broker for testing
	InMemoryBroker BrokerType = "memory"
)
