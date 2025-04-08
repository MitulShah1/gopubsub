package failover

import (
	"context"
	"errors"
	"time"
)

var (
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")
	ErrCircuitOpen        = errors.New("circuit breaker is open")
)

// RetryMetrics defines the interface for retry-specific metrics
type RetryMetrics interface {
	// RecordCircuitBreakerTrip records when the circuit breaker trips
	RecordCircuitBreakerTrip()
	// RecordCircuitBreakerReset records when the circuit breaker resets
	RecordCircuitBreakerReset()
}

// RetryConfig holds configuration for retry behavior
type RetryConfig struct {
	MaxRetries     int
	InitialDelay   time.Duration
	MaxDelay       time.Duration
	Multiplier     float64
	JitterFactor   float64
	CircuitBreaker *CircuitBreaker
	Metrics        RetryMetrics
}

// Validate checks if the retry configuration is valid
func (c *RetryConfig) Validate() error {
	if c.MaxRetries < 0 {
		return errors.New("max retries cannot be negative")
	}
	if c.InitialDelay < 0 {
		return errors.New("initial delay cannot be negative")
	}
	if c.MaxDelay < 0 {
		return errors.New("max delay cannot be negative")
	}
	if c.Multiplier <= 0 {
		return errors.New("multiplier must be positive")
	}
	if c.JitterFactor < 0 || c.JitterFactor > 1 {
		return errors.New("jitter factor must be between 0 and 1")
	}
	return nil
}

// ExecuteWithRetry executes a function with retry logic
func ExecuteWithRetry(ctx context.Context, config *RetryConfig, operation func() error) error {
	if config.CircuitBreaker != nil && config.Metrics != nil {
		config.Metrics.RecordCircuitBreakerTrip()
		if !config.CircuitBreaker.AllowRequest() {
			return ErrCircuitOpen
		}
	}

	var lastErr error
	delay := config.InitialDelay

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		if err := operation(); err != nil {
			lastErr = err
			if config.CircuitBreaker != nil && config.Metrics != nil {
				config.Metrics.RecordCircuitBreakerTrip()
			}

			if attempt == config.MaxRetries {
				return ErrMaxRetriesExceeded
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				delay = time.Duration(float64(delay) * config.Multiplier)
				if delay > config.MaxDelay {
					delay = config.MaxDelay
				}
				continue
			}
		}

		if config.CircuitBreaker != nil && config.Metrics != nil {
			config.Metrics.RecordCircuitBreakerReset()
		}
		return nil
	}

	return lastErr
}
