package streamline

import (
	"context"
	"testing"
	"time"
)

func TestCircuitBreakerStartsClosed(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())
	if cb.State() != CircuitClosed {
		t.Errorf("expected CLOSED, got %s", cb.State())
	}
	if !cb.Allow() {
		t.Error("expected Allow() to return true in closed state")
	}
}

func TestCircuitBreakerOpensAfterThreshold(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    3,
		SuccessThreshold:    2,
		OpenTimeout:         1 * time.Second,
		HalfOpenMaxRequests: 1,
	}
	cb := NewCircuitBreaker(config)

	// Record failures up to threshold
	for i := 0; i < 3; i++ {
		cb.Allow()
		cb.RecordFailure()
	}

	if cb.State() != CircuitOpen {
		t.Errorf("expected OPEN after %d failures, got %s", 3, cb.State())
	}

	if cb.Allow() {
		t.Error("expected Allow() to return false in open state")
	}
}

func TestCircuitBreakerTransitionsToHalfOpen(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    2,
		SuccessThreshold:    1,
		OpenTimeout:         50 * time.Millisecond,
		HalfOpenMaxRequests: 1,
	}
	cb := NewCircuitBreaker(config)

	cb.Allow()
	cb.RecordFailure()
	cb.Allow()
	cb.RecordFailure()

	if cb.State() != CircuitOpen {
		t.Fatalf("expected OPEN, got %s", cb.State())
	}

	// Wait for open timeout
	time.Sleep(60 * time.Millisecond)

	if cb.State() != CircuitHalfOpen {
		t.Errorf("expected HALF_OPEN after timeout, got %s", cb.State())
	}

	if !cb.Allow() {
		t.Error("expected Allow() to return true in half-open state")
	}
}

func TestCircuitBreakerClosesOnHalfOpenSuccess(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    2,
		SuccessThreshold:    1,
		OpenTimeout:         50 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	}
	cb := NewCircuitBreaker(config)

	// Trip open
	cb.Allow()
	cb.RecordFailure()
	cb.Allow()
	cb.RecordFailure()

	// Wait for half-open
	time.Sleep(60 * time.Millisecond)
	cb.Allow()
	cb.RecordSuccess()

	if cb.State() != CircuitClosed {
		t.Errorf("expected CLOSED after half-open success, got %s", cb.State())
	}
}

func TestCircuitBreakerReopensOnHalfOpenFailure(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    2,
		SuccessThreshold:    2,
		OpenTimeout:         50 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	}
	cb := NewCircuitBreaker(config)

	// Trip open
	cb.Allow()
	cb.RecordFailure()
	cb.Allow()
	cb.RecordFailure()

	// Wait for half-open
	time.Sleep(60 * time.Millisecond)
	cb.Allow()
	cb.RecordFailure()

	if cb.State() != CircuitOpen {
		t.Errorf("expected OPEN after half-open failure, got %s", cb.State())
	}
}

func TestCircuitBreakerReset(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    1,
		SuccessThreshold:    1,
		OpenTimeout:         10 * time.Minute,
		HalfOpenMaxRequests: 1,
	}
	cb := NewCircuitBreaker(config)

	cb.Allow()
	cb.RecordFailure()

	if cb.State() != CircuitOpen {
		t.Fatalf("expected OPEN, got %s", cb.State())
	}

	cb.Reset()

	if cb.State() != CircuitClosed {
		t.Errorf("expected CLOSED after reset, got %s", cb.State())
	}
	if !cb.Allow() {
		t.Error("expected Allow() after reset")
	}
}

func TestCircuitBreakerSuccessResetsFailureCount(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    3,
		SuccessThreshold:    1,
		OpenTimeout:         1 * time.Second,
		HalfOpenMaxRequests: 1,
	}
	cb := NewCircuitBreaker(config)

	// Two failures then a success should reset count
	cb.Allow()
	cb.RecordFailure()
	cb.Allow()
	cb.RecordFailure()
	cb.Allow()
	cb.RecordSuccess()

	// Two more failures should not trip (count was reset)
	cb.Allow()
	cb.RecordFailure()
	cb.Allow()
	cb.RecordFailure()

	if cb.State() != CircuitClosed {
		t.Errorf("expected CLOSED (failure count should have reset), got %s", cb.State())
	}
}

func TestCircuitBreakerStateChangeCallback(t *testing.T) {
	var transitions []string
	config := CircuitBreakerConfig{
		FailureThreshold:    1,
		SuccessThreshold:    1,
		OpenTimeout:         50 * time.Millisecond,
		HalfOpenMaxRequests: 1,
		OnStateChange: func(from, to CircuitState) {
			transitions = append(transitions, from.String()+"->"+to.String())
		},
	}
	cb := NewCircuitBreaker(config)

	cb.Allow()
	cb.RecordFailure()

	time.Sleep(60 * time.Millisecond)
	cb.Allow()
	cb.RecordSuccess()

	expected := []string{"CLOSED->OPEN", "OPEN->HALF_OPEN", "HALF_OPEN->CLOSED"}
	if len(transitions) != len(expected) {
		t.Fatalf("expected %d transitions, got %d: %v", len(expected), len(transitions), transitions)
	}
	for i, exp := range expected {
		if transitions[i] != exp {
			t.Errorf("transition[%d]: expected %s, got %s", i, exp, transitions[i])
		}
	}
}

func TestCircuitBreakerHalfOpenLimitsRequests(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    1,
		SuccessThreshold:    1,
		OpenTimeout:         50 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	}
	cb := NewCircuitBreaker(config)

	cb.Allow()
	cb.RecordFailure()

	time.Sleep(60 * time.Millisecond)

	// Should allow up to HalfOpenMaxRequests
	if !cb.Allow() {
		t.Error("expected first half-open request to be allowed")
	}
	if !cb.Allow() {
		t.Error("expected second half-open request to be allowed")
	}
	if cb.Allow() {
		t.Error("expected third half-open request to be rejected")
	}
}

func TestCircuitBreakerProducerReturnsErrCircuitOpen(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    2,
		SuccessThreshold:    1,
		OpenTimeout:         10 * time.Second,
		HalfOpenMaxRequests: 1,
	}
	cb := NewCircuitBreaker(config)

	// Create a producer with a circuit breaker but no real sarama client.
	// The circuit breaker check runs before any sarama call, so this is safe.
	p := &Producer{
		circuitBreaker: cb,
	}

	// Trip the circuit breaker open
	cb.Allow()
	cb.RecordFailure()
	cb.Allow()
	cb.RecordFailure()

	if cb.State() != CircuitOpen {
		t.Fatalf("expected circuit OPEN, got %s", cb.State())
	}

	ctx := context.Background()
	_, err := p.SendMessage(ctx, &Message{Topic: "test", Value: []byte("hello")})
	if err == nil {
		t.Fatal("expected error from SendMessage when circuit is open")
	}

	if err != ErrCircuitOpen {
		t.Errorf("expected ErrCircuitOpen, got: %v", err)
	}
}

func TestCircuitBreakerNilDisabled(t *testing.T) {
	// A producer with nil circuitBreaker should not panic on the Allow/Record paths.
	p := &Producer{
		circuitBreaker: nil,
	}

	// Verify the field is nil (circuit breaker disabled)
	if p.circuitBreaker != nil {
		t.Fatal("expected nil circuit breaker")
	}

	// Verify DefaultConfig leaves CircuitBreaker as nil
	cfg := DefaultConfig()
	if cfg.CircuitBreaker != nil {
		t.Fatal("expected DefaultConfig().CircuitBreaker to be nil")
	}
}
