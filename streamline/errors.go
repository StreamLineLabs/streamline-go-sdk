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
	// ErrRateLimited indicates the client is being rate-limited.
	ErrRateLimited
	// ErrInternal indicates an internal/unexpected error.
	ErrInternal
	// ErrContractViolation indicates a record violated a topic's data contract.
	ErrContractViolation
	// ErrAttestationFailed indicates attestation signature verification failed.
	ErrAttestationFailed
	// ErrMemoryAccessDenied indicates an agent lacks permission to access memory.
	ErrMemoryAccessDenied
	// ErrBranchQuotaExceeded indicates a branch exceeded its storage/lifetime quota.
	ErrBranchQuotaExceeded
	// ErrSemanticSearchUnavailable indicates semantic search is unavailable.
	ErrSemanticSearchUnavailable
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
	case ErrRateLimited:
		return "RATE_LIMITED"
	case ErrInternal:
		return "INTERNAL_ERROR"
	case ErrContractViolation:
		return "CONTRACT_VIOLATION"
	case ErrAttestationFailed:
		return "ATTESTATION_FAILED"
	case ErrMemoryAccessDenied:
		return "MEMORY_ACCESS_DENIED"
	case ErrBranchQuotaExceeded:
		return "BRANCH_QUOTA_EXCEEDED"
	case ErrSemanticSearchUnavailable:
		return "SEMANTIC_SEARCH_UNAVAILABLE"
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

// NewContractViolationError creates a contract violation error.
func NewContractViolationError(topic string, details string) *StreamlineError {
	return &StreamlineError{
		Code:      ErrContractViolation,
		Message:   fmt.Sprintf("contract violation on topic '%s': %s", topic, details),
		Hint:      "Validate the record against the topic's registered schema",
		Retryable: false,
	}
}

// NewAttestationFailedError creates an attestation verification error.
func NewAttestationFailedError(details string) *StreamlineError {
	return &StreamlineError{
		Code:      ErrAttestationFailed,
		Message:   fmt.Sprintf("attestation verification failed: %s", details),
		Hint:      "Check the signing key and attestation configuration",
		Retryable: false,
	}
}

// NewMemoryAccessDeniedError creates a memory access denied error.
func NewMemoryAccessDeniedError(agent string) *StreamlineError {
	return &StreamlineError{
		Code:      ErrMemoryAccessDenied,
		Message:   fmt.Sprintf("memory access denied for agent: %s", agent),
		Hint:      "Verify agent permissions for memory operations",
		Retryable: false,
	}
}

// NewBranchQuotaExceededError creates a branch quota exceeded error.
func NewBranchQuotaExceededError(branch string, details string) *StreamlineError {
	return &StreamlineError{
		Code:      ErrBranchQuotaExceeded,
		Message:   fmt.Sprintf("branch quota exceeded for '%s': %s", branch, details),
		Hint:      "Increase branch quotas or clean up unused branches",
		Retryable: false,
	}
}

// NewSemanticSearchUnavailableError creates a semantic search unavailable error.
func NewSemanticSearchUnavailableError(details string, cause error) *StreamlineError {
	return &StreamlineError{
		Code:      ErrSemanticSearchUnavailable,
		Message:   fmt.Sprintf("semantic search unavailable: %s", details),
		Hint:      "Check embedding provider connectivity and configuration",
		Retryable: true,
		Err:       cause,
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
