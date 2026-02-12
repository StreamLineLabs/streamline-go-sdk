package streamline

import (
	"errors"
	"fmt"
)

// ErrorCode represents the type of error that occurred.
type ErrorCode int

const (
	// ErrConnection indicates a connection failure.
	ErrConnection ErrorCode = iota + 1
	// ErrAuthentication indicates an authentication failure.
	ErrAuthentication
	// ErrAuthorization indicates an authorization/ACL failure.
	ErrAuthorization
	// ErrTopicNotFound indicates the requested topic does not exist.
	ErrTopicNotFound
	// ErrTimeout indicates an operation timed out.
	ErrTimeout
	// ErrProducer indicates a producer-related error.
	ErrProducer
	// ErrConsumer indicates a consumer-related error.
	ErrConsumer
	// ErrSerialization indicates a serialization/deserialization error.
	ErrSerialization
	// ErrConfiguration indicates an invalid configuration.
	ErrConfiguration
	// ErrInternal indicates an internal/unexpected error.
	ErrInternal
)

// String returns the string representation of an ErrorCode.
func (c ErrorCode) String() string {
	switch c {
	case ErrConnection:
		return "CONNECTION_ERROR"
	case ErrAuthentication:
		return "AUTHENTICATION_ERROR"
	case ErrAuthorization:
		return "AUTHORIZATION_ERROR"
	case ErrTopicNotFound:
		return "TOPIC_NOT_FOUND"
	case ErrTimeout:
		return "TIMEOUT"
	case ErrProducer:
		return "PRODUCER_ERROR"
	case ErrConsumer:
		return "CONSUMER_ERROR"
	case ErrSerialization:
		return "SERIALIZATION_ERROR"
	case ErrConfiguration:
		return "CONFIGURATION_ERROR"
	case ErrInternal:
		return "INTERNAL_ERROR"
	default:
		return "UNKNOWN_ERROR"
	}
}

// StreamlineError is the base error type for all Streamline SDK errors.
// It provides an error code for programmatic error handling and an
// optional hint for resolving the error.
type StreamlineError struct {
	// Code is the error category.
	Code ErrorCode
	// Message is the human-readable error description.
	Message string
	// Hint is an optional suggestion for resolving the error.
	Hint string
	// Retryable indicates whether the operation can be retried.
	Retryable bool
	// Err is the underlying cause, if any.
	Err error
}

// Error implements the error interface.
func (e *StreamlineError) Error() string {
	s := fmt.Sprintf("streamline [%s]: %s", e.Code, e.Message)
	if e.Hint != "" {
		s += fmt.Sprintf(" (hint: %s)", e.Hint)
	}
	return s
}

// Unwrap returns the underlying error for errors.Is/As support.
func (e *StreamlineError) Unwrap() error {
	return e.Err
}

// NewError creates a new StreamlineError.
func NewError(code ErrorCode, message string) *StreamlineError {
	return &StreamlineError{
		Code:    code,
		Message: message,
	}
}

// NewConnectionError creates a connection error.
func NewConnectionError(message string, cause error) *StreamlineError {
	return &StreamlineError{
		Code:      ErrConnection,
		Message:   message,
		Hint:      "Check that Streamline server is running and accessible",
		Retryable: true,
		Err:       cause,
	}
}

// NewAuthenticationError creates an authentication error.
func NewAuthenticationError(message string, cause error) *StreamlineError {
	return &StreamlineError{
		Code:      ErrAuthentication,
		Message:   message,
		Hint:      "Verify your SASL credentials and mechanism",
		Retryable: false,
		Err:       cause,
	}
}

// NewTopicNotFoundError creates a topic-not-found error.
func NewTopicNotFoundError(topic string) *StreamlineError {
	return &StreamlineError{
		Code:      ErrTopicNotFound,
		Message:   fmt.Sprintf("topic not found: %s", topic),
		Hint:      fmt.Sprintf("Create the topic with: streamline-cli topics create %s", topic),
		Retryable: false,
	}
}

// NewTimeoutError creates a timeout error.
func NewTimeoutError(operation string, cause error) *StreamlineError {
	return &StreamlineError{
		Code:      ErrTimeout,
		Message:   fmt.Sprintf("operation timed out: %s", operation),
		Hint:      "Consider increasing timeout settings or checking server load",
		Retryable: true,
		Err:       cause,
	}
}

// NewProducerError creates a producer error.
func NewProducerError(message string, cause error) *StreamlineError {
	return &StreamlineError{
		Code:      ErrProducer,
		Message:   message,
		Retryable: true,
		Err:       cause,
	}
}

// NewConsumerError creates a consumer error.
func NewConsumerError(message string, cause error) *StreamlineError {
	return &StreamlineError{
		Code:      ErrConsumer,
		Message:   message,
		Retryable: true,
		Err:       cause,
	}
}

// NewConfigurationError creates a configuration error.
func NewConfigurationError(message string) *StreamlineError {
	return &StreamlineError{
		Code:      ErrConfiguration,
		Message:   message,
		Retryable: false,
	}
}

// IsStreamlineError checks if an error is a StreamlineError.
func IsStreamlineError(err error) bool {
	var se *StreamlineError
	return errors.As(err, &se)
}

// GetErrorCode extracts the error code from an error, or 0 if not a StreamlineError.
func GetErrorCode(err error) ErrorCode {
	var se *StreamlineError
	if errors.As(err, &se) {
		return se.Code
	}
	return 0
}

// IsRetryable checks if an error is retryable.
func IsRetryable(err error) bool {
	var se *StreamlineError
	if errors.As(err, &se) {
		return se.Retryable
	}
	return false
}
