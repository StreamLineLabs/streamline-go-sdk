package streamline

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// HealthStatus represents the health of a connection.
type HealthStatus struct {
	Healthy   bool          `json:"healthy"`
	Latency   time.Duration `json:"latency"`
	Broker    string        `json:"broker"`
	Timestamp time.Time     `json:"timestamp"`
}

// HealthCheck performs a health check against the connected broker.
func (c *Client) HealthCheck(ctx context.Context) (*HealthStatus, error) {
	start := time.Now()

	// Attempt metadata fetch as a connectivity check
	_, err := c.Admin.ListTopics(ctx)
	latency := time.Since(start)

	broker := strings.Join(c.config.Brokers, ",")

	if err != nil {
		return &HealthStatus{
			Healthy:   false,
			Latency:   latency,
			Broker:    broker,
			Timestamp: time.Now(),
		}, fmt.Errorf("health check failed: %w", err)
	}

	return &HealthStatus{
		Healthy:   true,
		Latency:   latency,
		Broker:    broker,
		Timestamp: time.Now(),
	}, nil
}
