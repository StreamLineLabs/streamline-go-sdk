package streamline

import (
	"fmt"
	"testing"
	"time"
)

func TestMessageCreation(t *testing.T) {
	msg := &Message{
		Topic: "test-topic",
		Key:   []byte("key-1"),
		Value: []byte("hello world"),
	}

	if msg.Topic != "test-topic" {
		t.Errorf("Topic = %q, want 'test-topic'", msg.Topic)
	}
	if string(msg.Key) != "key-1" {
		t.Errorf("Key = %q, want 'key-1'", msg.Key)
	}
	if string(msg.Value) != "hello world" {
		t.Errorf("Value = %q, want 'hello world'", msg.Value)
	}
}

func TestMessageWithHeaders(t *testing.T) {
	headers := map[string][]byte{
		"content-type": []byte("application/json"),
		"trace-id":     []byte("abc-123"),
		"correlation":  []byte("xyz-789"),
	}

	msg := &Message{
		Topic:   "events",
		Key:     []byte("key"),
		Value:   []byte(`{"event":"test"}`),
		Headers: headers,
	}

	if len(msg.Headers) != 3 {
		t.Fatalf("expected 3 headers, got %d", len(msg.Headers))
	}

	for k, want := range headers {
		got, ok := msg.Headers[k]
		if !ok {
			t.Errorf("missing header %q", k)
			continue
		}
		if string(got) != string(want) {
			t.Errorf("header[%q] = %q, want %q", k, got, want)
		}
	}
}

func TestMessageWithPartition(t *testing.T) {
	tests := []struct {
		name      string
		partition int32
	}{
		{"auto partition", -1},
		{"partition 0", 0},
		{"partition 5", 5},
		{"partition 99", 99},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &Message{
				Topic:     "topic",
				Value:     []byte("data"),
				Partition: tt.partition,
			}
			if msg.Partition != tt.partition {
				t.Errorf("Partition = %d, want %d", msg.Partition, tt.partition)
			}
		})
	}
}

func TestMessageWithTimestamp(t *testing.T) {
	now := time.Now()
	msg := &Message{
		Topic:     "topic",
		Value:     []byte("data"),
		Timestamp: now,
	}

	if msg.Timestamp != now {
		t.Errorf("Timestamp = %v, want %v", msg.Timestamp, now)
	}

	// Zero timestamp
	msg2 := &Message{Topic: "topic", Value: []byte("data")}
	if !msg2.Timestamp.IsZero() {
		t.Error("expected zero timestamp by default")
	}
}

func TestMessageNilKeyAndValue(t *testing.T) {
	msg := &Message{Topic: "topic"}
	if msg.Key != nil {
		t.Error("expected nil key")
	}
	if msg.Value != nil {
		t.Error("expected nil value")
	}
	if msg.Headers != nil {
		t.Error("expected nil headers")
	}
}

func TestProducerResult(t *testing.T) {
	now := time.Now()
	result := &ProducerResult{
		Topic:     "output-topic",
		Partition: 3,
		Offset:    42,
		Timestamp: now,
	}

	if result.Topic != "output-topic" {
		t.Errorf("Topic = %q, want 'output-topic'", result.Topic)
	}
	if result.Partition != 3 {
		t.Errorf("Partition = %d, want 3", result.Partition)
	}
	if result.Offset != 42 {
		t.Errorf("Offset = %d, want 42", result.Offset)
	}
	if result.Timestamp != now {
		t.Errorf("Timestamp = %v, want %v", result.Timestamp, now)
	}
}

func TestAsyncProducerResult(t *testing.T) {
	tests := []struct {
		name      string
		result    AsyncProducerResult
		hasErr    bool
	}{
		{
			name: "success",
			result: AsyncProducerResult{
				Topic:     "async-topic",
				Partition: 1,
				Offset:    100,
				Timestamp: time.Now(),
			},
			hasErr: false,
		},
		{
			name: "error",
			result: AsyncProducerResult{
				Err: fmt.Errorf("send failed"),
			},
			hasErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if (tt.result.Err != nil) != tt.hasErr {
				t.Errorf("Err = %v, wantErr = %v", tt.result.Err, tt.hasErr)
			}
			if !tt.hasErr {
				if tt.result.Topic != "async-topic" {
					t.Errorf("Topic = %q, want 'async-topic'", tt.result.Topic)
				}
				if tt.result.Partition != 1 {
					t.Errorf("Partition = %d, want 1", tt.result.Partition)
				}
				if tt.result.Offset != 100 {
					t.Errorf("Offset = %d, want 100", tt.result.Offset)
				}
			}
		})
	}
}

func TestProducerCloseIdempotent(t *testing.T) {
	p := &Producer{closed: true}
	if err := p.Close(); err != nil {
		t.Errorf("Close on already-closed producer should return nil, got %v", err)
	}
}

func TestProducerSendWhenClosed(t *testing.T) {
	p := &Producer{closed: true}
	_, err := p.SendMessage(nil, &Message{Topic: "topic", Value: []byte("data")})
	if err == nil {
		t.Fatal("expected error when producer is closed")
	}
	if err.Error() != "streamline: producer is closed" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestProducerSendAsyncWhenClosed(t *testing.T) {
	p := &Producer{closed: true}
	ch := p.SendAsync(&Message{Topic: "topic", Value: []byte("data")})

	result := <-ch
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Err == nil {
		t.Fatal("expected error in async result when producer is closed")
	}
	if result.Err.Error() != "streamline: producer is closed" {
		t.Errorf("unexpected error: %v", result.Err)
	}
}

func TestProducerFlush(t *testing.T) {
	p := &Producer{}
	if err := p.Flush(time.Second); err != nil {
		t.Errorf("Flush should return nil, got %v", err)
	}
}
