package streamline

import (
	"sync"
	"time"
)

// CircuitState represents the current state of the circuit breaker.
type CircuitState int

const (
	// CircuitClosed means requests flow normally.
	CircuitClosed CircuitState = iota
	// CircuitOpen means requests are rejected immediately.
	CircuitOpen
	// CircuitHalfOpen means a limited number of probe requests are allowed.
	CircuitHalfOpen
)

// String returns the string representation of a CircuitState.
func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "CLOSED"
	case CircuitOpen:
		return "OPEN"
	case CircuitHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

// CircuitBreakerConfig configures the circuit breaker behavior.
type CircuitBreakerConfig struct {
	// FailureThreshold is the number of consecutive failures before opening the circuit.
	FailureThreshold int
	// SuccessThreshold is the number of consecutive successes in half-open state
	// required to close the circuit.
	SuccessThreshold int
	// OpenTimeout is how long to wait before transitioning from open to half-open.
	OpenTimeout time.Duration
	// HalfOpenMaxRequests is the maximum number of probe requests allowed in half-open state.
	HalfOpenMaxRequests int
	// OnStateChange is called when the circuit state changes (optional).
	OnStateChange func(from, to CircuitState)
}

// DefaultCircuitBreakerConfig returns sensible defaults for the circuit breaker.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureThreshold:    5,
		SuccessThreshold:    2,
		OpenTimeout:         30 * time.Second,
		HalfOpenMaxRequests: 3,
	}
}

// CircuitBreaker implements the circuit breaker pattern for resilient operations.
// It tracks failures and temporarily stops requests to a failing service,
// allowing it time to recover before retrying.
type CircuitBreaker struct {
	mu     sync.Mutex
	config CircuitBreakerConfig
	state  CircuitState

	failureCount   int
	successCount   int
	halfOpenCount  int
	lastFailureAt  time.Time
	lastStateChange time.Time
}

// NewCircuitBreaker creates a new circuit breaker with the given configuration.
func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	if config.FailureThreshold <= 0 {
		config.FailureThreshold = 5
	}
	if config.SuccessThreshold <= 0 {
		config.SuccessThreshold = 2
	}
	if config.OpenTimeout <= 0 {
		config.OpenTimeout = 30 * time.Second
	}
	if config.HalfOpenMaxRequests <= 0 {
		config.HalfOpenMaxRequests = 3
	}

	return &CircuitBreaker{
		config:          config,
		state:           CircuitClosed,
		lastStateChange: time.Now(),
	}
}

// Allow checks if a request should be allowed through the circuit breaker.
// Returns true if the request can proceed, false if the circuit is open.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if time.Since(cb.lastFailureAt) >= cb.config.OpenTimeout {
			cb.transition(CircuitHalfOpen)
			cb.halfOpenCount = 1
			return true
		}
		return false
	case CircuitHalfOpen:
		if cb.halfOpenCount < cb.config.HalfOpenMaxRequests {
			cb.halfOpenCount++
			return true
		}
		return false
	default:
		return true
	}
}

// RecordSuccess records a successful request.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		cb.failureCount = 0
	case CircuitHalfOpen:
		cb.successCount++
		if cb.successCount >= cb.config.SuccessThreshold {
			cb.transition(CircuitClosed)
		}
	}
}

// RecordFailure records a failed request. Only retryable errors should be
// recorded as failures — non-retryable errors (auth, not found) should not
// trip the circuit breaker.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.lastFailureAt = time.Now()

	switch cb.state {
	case CircuitClosed:
		cb.failureCount++
		if cb.failureCount >= cb.config.FailureThreshold {
			cb.transition(CircuitOpen)
		}
	case CircuitHalfOpen:
		cb.transition(CircuitOpen)
	}
}

// State returns the current circuit breaker state.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Auto-transition from open to half-open if timeout has elapsed
	if cb.state == CircuitOpen && time.Since(cb.lastFailureAt) >= cb.config.OpenTimeout {
		cb.transition(CircuitHalfOpen)
	}

	return cb.state
}

// Reset manually resets the circuit breaker to closed state.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.transition(CircuitClosed)
}

// Counts returns the current failure and success counts.
func (cb *CircuitBreaker) Counts() (failures, successes int) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.failureCount, cb.successCount
}

func (cb *CircuitBreaker) transition(to CircuitState) {
	from := cb.state
	if from == to {
		return
	}

	cb.state = to
	cb.lastStateChange = time.Now()
	cb.failureCount = 0
	cb.successCount = 0
	cb.halfOpenCount = 0

	if cb.config.OnStateChange != nil {
		cb.config.OnStateChange(from, to)
	}
}

// ErrCircuitOpen is returned when the circuit breaker is open and rejecting requests.
var ErrCircuitOpen = &StreamlineError{
	Code:      ErrConnection,
	Message:   "circuit breaker is open — too many recent failures",
	Hint:      "The client detected repeated failures and is temporarily pausing requests. It will retry automatically.",
	Retryable: true,
}
