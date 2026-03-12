package streamline

import (
	"sync"
	"sync/atomic"
	"time"
)

// ClientMetrics tracks SDK health and performance metrics.
type ClientMetrics struct {
	messagesProduced atomic.Int64
	messagesConsumed atomic.Int64
	bytesSent        atomic.Int64
	bytesReceived    atomic.Int64
	errorsTotal      atomic.Int64

	mu                  sync.Mutex
	produceLatencySum   float64
	produceLatencyCount int64
	consumeLatencySum   float64
	consumeLatencyCount int64

	startTime time.Time
}

// MetricsSnapshot is a point-in-time view of client metrics.
type MetricsSnapshot struct {
	MessagesProduced    int64   `json:"messages_produced"`
	MessagesConsumed    int64   `json:"messages_consumed"`
	BytesSent           int64   `json:"bytes_sent"`
	BytesReceived       int64   `json:"bytes_received"`
	ErrorsTotal         int64   `json:"errors_total"`
	ProduceLatencyAvgMs float64 `json:"produce_latency_avg_ms"`
	ConsumeLatencyAvgMs float64 `json:"consume_latency_avg_ms"`
	UptimeMs            int64   `json:"uptime_ms"`
}

// NewClientMetrics creates a new metrics collector.
func NewClientMetrics() *ClientMetrics {
	return &ClientMetrics{
		startTime: time.Now(),
	}
}

// RecordProduce records a successful produce operation.
func (m *ClientMetrics) RecordProduce(messageCount int64, bytes int64, latencyMs float64) {
	m.messagesProduced.Add(messageCount)
	m.bytesSent.Add(bytes)
	m.mu.Lock()
	m.produceLatencySum += latencyMs
	m.produceLatencyCount++
	m.mu.Unlock()
}

// RecordConsume records a successful consume operation.
func (m *ClientMetrics) RecordConsume(messageCount int64, bytes int64, latencyMs float64) {
	m.messagesConsumed.Add(messageCount)
	m.bytesReceived.Add(bytes)
	m.mu.Lock()
	m.consumeLatencySum += latencyMs
	m.consumeLatencyCount++
	m.mu.Unlock()
}

// RecordError records an error occurrence.
func (m *ClientMetrics) RecordError() {
	m.errorsTotal.Add(1)
}

// Snapshot returns a point-in-time view of all metrics.
func (m *ClientMetrics) Snapshot() MetricsSnapshot {
	m.mu.Lock()
	var produceAvg, consumeAvg float64
	if m.produceLatencyCount > 0 {
		produceAvg = m.produceLatencySum / float64(m.produceLatencyCount)
	}
	if m.consumeLatencyCount > 0 {
		consumeAvg = m.consumeLatencySum / float64(m.consumeLatencyCount)
	}
	m.mu.Unlock()

	return MetricsSnapshot{
		MessagesProduced:    m.messagesProduced.Load(),
		MessagesConsumed:    m.messagesConsumed.Load(),
		BytesSent:           m.bytesSent.Load(),
		BytesReceived:       m.bytesReceived.Load(),
		ErrorsTotal:         m.errorsTotal.Load(),
		ProduceLatencyAvgMs: produceAvg,
		ConsumeLatencyAvgMs: consumeAvg,
		UptimeMs:            time.Since(m.startTime).Milliseconds(),
	}
}

// Reset resets all metrics to zero.
func (m *ClientMetrics) Reset() {
	m.messagesProduced.Store(0)
	m.messagesConsumed.Store(0)
	m.bytesSent.Store(0)
	m.bytesReceived.Store(0)
	m.errorsTotal.Store(0)
	m.mu.Lock()
	m.produceLatencySum = 0
	m.produceLatencyCount = 0
	m.consumeLatencySum = 0
	m.consumeLatencyCount = 0
	m.mu.Unlock()
	m.startTime = time.Now()
}
