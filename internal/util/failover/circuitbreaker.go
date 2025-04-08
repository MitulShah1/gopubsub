package failover

import (
	"errors"
	"time"
)

// CircuitBreakerConfig holds configuration for circuit breaker
type CircuitBreakerConfig struct {
	FailureThreshold int
	ResetTimeout     time.Duration
	HalfOpenRequests int
	Metrics          RetryMetrics
}

// Validate checks if the circuit breaker configuration is valid
func (c *CircuitBreakerConfig) Validate() error {
	if c.FailureThreshold <= 0 {
		return errors.New("failure threshold must be positive")
	}
	if c.ResetTimeout <= 0 {
		return errors.New("reset timeout must be positive")
	}
	if c.HalfOpenRequests <= 0 {
		return errors.New("half-open requests must be positive")
	}
	return nil
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	config      *CircuitBreakerConfig
	failures    int
	lastFailure time.Time
	state       string // "closed", "open", "half-open"
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(config *CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		config: config,
		state:  "closed",
	}
}

// Trip opens the circuit breaker
func (cb *CircuitBreaker) Trip() {
	cb.state = "open"
	cb.lastFailure = time.Now()
	cb.failures++
}

// Reset closes the circuit breaker
func (cb *CircuitBreaker) Reset() {
	cb.state = "closed"
	cb.failures = 0
}

// AllowRequest checks if a request should be allowed
func (cb *CircuitBreaker) AllowRequest() bool {
	switch cb.state {
	case "closed":
		return true
	case "open":
		if time.Since(cb.lastFailure) > cb.config.ResetTimeout {
			cb.state = "half-open"
			return true
		}
		return false
	case "half-open":
		return cb.failures < cb.config.HalfOpenRequests
	default:
		return false
	}
}
