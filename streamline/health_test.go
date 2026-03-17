package streamline

import (
	"testing"
	"time"
)

func TestHealthStatusFields(t *testing.T) {
	now := time.Now()
	status := HealthStatus{
		Healthy:   true,
		Latency:   15 * time.Millisecond,
		Broker:    "localhost:9092",
		Timestamp: now,
	}

	if !status.Healthy {
		t.Error("expected Healthy = true")
	}
	if status.Latency != 15*time.Millisecond {
		t.Errorf("Latency = %v, want 15ms", status.Latency)
	}
	if status.Broker != "localhost:9092" {
		t.Errorf("Broker = %q, want %q", status.Broker, "localhost:9092")
	}
	if !status.Timestamp.Equal(now) {
		t.Errorf("Timestamp = %v, want %v", status.Timestamp, now)
	}
}

func TestHealthStatusUnhealthy(t *testing.T) {
	status := HealthStatus{
		Healthy:   false,
		Latency:   0,
		Broker:    "unreachable:9092",
		Timestamp: time.Now(),
	}

	if status.Healthy {
		t.Error("expected Healthy = false for unreachable broker")
	}
}

func TestHealthStatusMultipleBrokers(t *testing.T) {
	status := HealthStatus{
		Healthy:   true,
		Latency:   5 * time.Millisecond,
		Broker:    "broker1:9092,broker2:9092,broker3:9092",
		Timestamp: time.Now(),
	}

	if status.Broker != "broker1:9092,broker2:9092,broker3:9092" {
		t.Errorf("Broker = %q, expected comma-separated list", status.Broker)
	}
}
