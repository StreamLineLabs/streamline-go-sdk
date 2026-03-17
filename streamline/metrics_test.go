package streamline

import (
	"encoding/json"
	"sync"
	"testing"
)

func TestNewClientMetrics(t *testing.T) {
	m := NewClientMetrics()
	if m == nil {
		t.Fatal("NewClientMetrics returned nil")
	}
	snap := m.Snapshot()
	if snap.MessagesProduced != 0 {
		t.Errorf("initial MessagesProduced = %d, want 0", snap.MessagesProduced)
	}
	if snap.MessagesConsumed != 0 {
		t.Errorf("initial MessagesConsumed = %d, want 0", snap.MessagesConsumed)
	}
	if snap.BytesSent != 0 {
		t.Errorf("initial BytesSent = %d, want 0", snap.BytesSent)
	}
	if snap.BytesReceived != 0 {
		t.Errorf("initial BytesReceived = %d, want 0", snap.BytesReceived)
	}
	if snap.ErrorsTotal != 0 {
		t.Errorf("initial ErrorsTotal = %d, want 0", snap.ErrorsTotal)
	}
	if snap.ProduceLatencyAvgMs != 0 {
		t.Errorf("initial ProduceLatencyAvgMs = %f, want 0", snap.ProduceLatencyAvgMs)
	}
	if snap.ConsumeLatencyAvgMs != 0 {
		t.Errorf("initial ConsumeLatencyAvgMs = %f, want 0", snap.ConsumeLatencyAvgMs)
	}
}

func TestRecordProduce(t *testing.T) {
	m := NewClientMetrics()
	m.RecordProduce(5, 1024, 10.0)

	snap := m.Snapshot()
	if snap.MessagesProduced != 5 {
		t.Errorf("MessagesProduced = %d, want 5", snap.MessagesProduced)
	}
	if snap.BytesSent != 1024 {
		t.Errorf("BytesSent = %d, want 1024", snap.BytesSent)
	}
}

func TestRecordProduceAccumulates(t *testing.T) {
	m := NewClientMetrics()
	m.RecordProduce(3, 100, 5.0)
	m.RecordProduce(7, 200, 15.0)

	snap := m.Snapshot()
	if snap.MessagesProduced != 10 {
		t.Errorf("MessagesProduced = %d, want 10", snap.MessagesProduced)
	}
	if snap.BytesSent != 300 {
		t.Errorf("BytesSent = %d, want 300", snap.BytesSent)
	}
}

func TestRecordConsume(t *testing.T) {
	m := NewClientMetrics()
	m.RecordConsume(10, 4096, 15.0)

	snap := m.Snapshot()
	if snap.MessagesConsumed != 10 {
		t.Errorf("MessagesConsumed = %d, want 10", snap.MessagesConsumed)
	}
	if snap.BytesReceived != 4096 {
		t.Errorf("BytesReceived = %d, want 4096", snap.BytesReceived)
	}
}

func TestRecordConsumeAccumulates(t *testing.T) {
	m := NewClientMetrics()
	m.RecordConsume(5, 512, 8.0)
	m.RecordConsume(15, 1024, 12.0)

	snap := m.Snapshot()
	if snap.MessagesConsumed != 20 {
		t.Errorf("MessagesConsumed = %d, want 20", snap.MessagesConsumed)
	}
	if snap.BytesReceived != 1536 {
		t.Errorf("BytesReceived = %d, want 1536", snap.BytesReceived)
	}
}

func TestRecordError(t *testing.T) {
	m := NewClientMetrics()
	m.RecordError()
	m.RecordError()
	m.RecordError()

	snap := m.Snapshot()
	if snap.ErrorsTotal != 3 {
		t.Errorf("ErrorsTotal = %d, want 3", snap.ErrorsTotal)
	}
}

func TestProduceLatencyAverage(t *testing.T) {
	m := NewClientMetrics()
	m.RecordProduce(1, 100, 10.0)
	m.RecordProduce(1, 100, 20.0)
	m.RecordProduce(1, 100, 30.0)

	snap := m.Snapshot()
	expected := 20.0
	if snap.ProduceLatencyAvgMs != expected {
		t.Errorf("ProduceLatencyAvgMs = %f, want %f", snap.ProduceLatencyAvgMs, expected)
	}
}

func TestConsumeLatencyAverage(t *testing.T) {
	m := NewClientMetrics()
	m.RecordConsume(1, 100, 5.0)
	m.RecordConsume(1, 100, 15.0)

	snap := m.Snapshot()
	expected := 10.0
	if snap.ConsumeLatencyAvgMs != expected {
		t.Errorf("ConsumeLatencyAvgMs = %f, want %f", snap.ConsumeLatencyAvgMs, expected)
	}
}

func TestUptimeIsPositive(t *testing.T) {
	m := NewClientMetrics()
	snap := m.Snapshot()
	if snap.UptimeMs < 0 {
		t.Errorf("UptimeMs = %d, want >= 0", snap.UptimeMs)
	}
}

func TestReset(t *testing.T) {
	m := NewClientMetrics()
	m.RecordProduce(10, 1024, 5.0)
	m.RecordConsume(20, 2048, 10.0)
	m.RecordError()

	m.Reset()
	snap := m.Snapshot()

	if snap.MessagesProduced != 0 {
		t.Errorf("after Reset, MessagesProduced = %d, want 0", snap.MessagesProduced)
	}
	if snap.MessagesConsumed != 0 {
		t.Errorf("after Reset, MessagesConsumed = %d, want 0", snap.MessagesConsumed)
	}
	if snap.BytesSent != 0 {
		t.Errorf("after Reset, BytesSent = %d, want 0", snap.BytesSent)
	}
	if snap.BytesReceived != 0 {
		t.Errorf("after Reset, BytesReceived = %d, want 0", snap.BytesReceived)
	}
	if snap.ErrorsTotal != 0 {
		t.Errorf("after Reset, ErrorsTotal = %d, want 0", snap.ErrorsTotal)
	}
	if snap.ProduceLatencyAvgMs != 0 {
		t.Errorf("after Reset, ProduceLatencyAvgMs = %f, want 0", snap.ProduceLatencyAvgMs)
	}
	if snap.ConsumeLatencyAvgMs != 0 {
		t.Errorf("after Reset, ConsumeLatencyAvgMs = %f, want 0", snap.ConsumeLatencyAvgMs)
	}
}

func TestSnapshotIsIndependent(t *testing.T) {
	m := NewClientMetrics()
	snap1 := m.Snapshot()
	m.RecordProduce(5, 100, 10.0)
	snap2 := m.Snapshot()

	if snap1.MessagesProduced != 0 {
		t.Errorf("snap1.MessagesProduced = %d, want 0", snap1.MessagesProduced)
	}
	if snap2.MessagesProduced != 5 {
		t.Errorf("snap2.MessagesProduced = %d, want 5", snap2.MessagesProduced)
	}
}

func TestSnapshotJSONSerialization(t *testing.T) {
	m := NewClientMetrics()
	m.RecordProduce(42, 1024, 5.5)
	m.RecordConsume(100, 8192, 12.3)
	m.RecordError()

	snap := m.Snapshot()
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var decoded MetricsSnapshot
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if decoded.MessagesProduced != 42 {
		t.Errorf("decoded MessagesProduced = %d, want 42", decoded.MessagesProduced)
	}
	if decoded.BytesReceived != 8192 {
		t.Errorf("decoded BytesReceived = %d, want 8192", decoded.BytesReceived)
	}
	if decoded.ErrorsTotal != 1 {
		t.Errorf("decoded ErrorsTotal = %d, want 1", decoded.ErrorsTotal)
	}
}

func TestConcurrentMetricsAccess(t *testing.T) {
	m := NewClientMetrics()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			m.RecordProduce(1, 10, 1.0)
		}()
		go func() {
			defer wg.Done()
			m.RecordConsume(1, 10, 1.0)
		}()
		go func() {
			defer wg.Done()
			m.RecordError()
		}()
	}
	wg.Wait()

	snap := m.Snapshot()
	if snap.MessagesProduced != 100 {
		t.Errorf("concurrent MessagesProduced = %d, want 100", snap.MessagesProduced)
	}
	if snap.MessagesConsumed != 100 {
		t.Errorf("concurrent MessagesConsumed = %d, want 100", snap.MessagesConsumed)
	}
	if snap.ErrorsTotal != 100 {
		t.Errorf("concurrent ErrorsTotal = %d, want 100", snap.ErrorsTotal)
	}
}

func TestMixedProduceConsumeLatency(t *testing.T) {
	m := NewClientMetrics()
	m.RecordProduce(1, 100, 10.0)
	m.RecordConsume(1, 200, 20.0)

	snap := m.Snapshot()
	if snap.ProduceLatencyAvgMs != 10.0 {
		t.Errorf("ProduceLatencyAvgMs = %f, want 10.0", snap.ProduceLatencyAvgMs)
	}
	if snap.ConsumeLatencyAvgMs != 20.0 {
		t.Errorf("ConsumeLatencyAvgMs = %f, want 20.0", snap.ConsumeLatencyAvgMs)
	}
}
