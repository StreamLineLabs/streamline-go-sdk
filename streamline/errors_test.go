package streamline

import (
	"errors"
	"fmt"
	"testing"
)

func TestErrorCodeString(t *testing.T) {
	tests := []struct {
		code     ErrorCode
		expected string
	}{
		{ErrConnection, "CONNECTION_ERROR"},
		{ErrAuthentication, "AUTHENTICATION_ERROR"},
		{ErrAuthorization, "AUTHORIZATION_ERROR"},
		{ErrTopicNotFound, "TOPIC_NOT_FOUND"},
		{ErrTimeout, "TIMEOUT"},
		{ErrProducer, "PRODUCER_ERROR"},
		{ErrConsumer, "CONSUMER_ERROR"},
		{ErrSerialization, "SERIALIZATION_ERROR"},
		{ErrConfiguration, "CONFIGURATION_ERROR"},
		{ErrInternal, "INTERNAL_ERROR"},
		{ErrorCode(999), "UNKNOWN_ERROR"},
	}

	for _, tt := range tests {
		if got := tt.code.String(); got != tt.expected {
			t.Errorf("ErrorCode(%d).String() = %q, want %q", tt.code, got, tt.expected)
		}
	}
}

func TestStreamlineErrorMessage(t *testing.T) {
	err := NewError(ErrConnection, "connection refused")
	if err.Error() != "streamline [CONNECTION_ERROR]: connection refused" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
}

func TestStreamlineErrorWithHint(t *testing.T) {
	err := &StreamlineError{
		Code:    ErrTopicNotFound,
		Message: "topic not found: events",
		Hint:    "create the topic first",
	}
	msg := err.Error()
	if msg != "streamline [TOPIC_NOT_FOUND]: topic not found: events (hint: create the topic first)" {
		t.Errorf("unexpected error message: %s", msg)
	}
}

func TestStreamlineErrorUnwrap(t *testing.T) {
	cause := fmt.Errorf("dial tcp: connection refused")
	err := NewConnectionError("failed to connect", cause)

	if !errors.Is(err, cause) {
		t.Error("errors.Is should find the cause via Unwrap")
	}
}

func TestErrorsAs(t *testing.T) {
	err := NewConnectionError("cannot reach server", nil)
	var wrapped error = fmt.Errorf("wrapper: %w", err)

	var se *StreamlineError
	if !errors.As(wrapped, &se) {
		t.Fatal("errors.As should find *StreamlineError")
	}
	if se.Code != ErrConnection {
		t.Errorf("expected ErrConnection, got %v", se.Code)
	}
}

func TestIsStreamlineError(t *testing.T) {
	if IsStreamlineError(fmt.Errorf("plain error")) {
		t.Error("plain error should not be a StreamlineError")
	}
	if !IsStreamlineError(NewError(ErrInternal, "something went wrong")) {
		t.Error("StreamlineError should be recognized")
	}
}

func TestGetErrorCode(t *testing.T) {
	if code := GetErrorCode(fmt.Errorf("plain")); code != 0 {
		t.Errorf("expected 0 for non-StreamlineError, got %d", code)
	}
	if code := GetErrorCode(NewTimeoutError("fetch", nil)); code != ErrTimeout {
		t.Errorf("expected ErrTimeout, got %v", code)
	}
}

func TestIsRetryable(t *testing.T) {
	if IsRetryable(fmt.Errorf("plain")) {
		t.Error("plain error should not be retryable")
	}
	if !IsRetryable(NewConnectionError("conn refused", nil)) {
		t.Error("connection errors should be retryable")
	}
	if IsRetryable(NewAuthenticationError("bad creds", nil)) {
		t.Error("auth errors should not be retryable")
	}
	if IsRetryable(NewConfigurationError("bad config")) {
		t.Error("config errors should not be retryable")
	}
	if !IsRetryable(NewTimeoutError("fetch", nil)) {
		t.Error("timeout errors should be retryable")
	}
	if !IsRetryable(NewProducerError("send failed", nil)) {
		t.Error("producer errors should be retryable")
	}
}

func TestNewTopicNotFoundError(t *testing.T) {
	err := NewTopicNotFoundError("events")
	if err.Code != ErrTopicNotFound {
		t.Errorf("expected ErrTopicNotFound, got %v", err.Code)
	}
	if err.Hint == "" {
		t.Error("topic not found error should have a hint")
	}
	if err.Retryable {
		t.Error("topic not found should not be retryable")
	}
}

func TestNewConnectionError(t *testing.T) {
	cause := fmt.Errorf("dial failed")
	err := NewConnectionError("cannot connect", cause)
	if err.Code != ErrConnection {
		t.Errorf("expected ErrConnection, got %v", err.Code)
	}
	if !err.Retryable {
		t.Error("connection error should be retryable")
	}
	if err.Unwrap() != cause {
		t.Error("Unwrap should return the cause")
	}
}
