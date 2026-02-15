package streamline

import (
	"context"
	"testing"
	"time"
)

func TestConsumerMessage(t *testing.T) {
	now := time.Now()
	msg := &ConsumerMessage{
		Topic:     "events",
		Partition: 2,
		Offset:    1000,
		Key:       []byte("key-1"),
		Value:     []byte(`{"event":"click"}`),
		Timestamp: now,
	}

	if msg.Topic != "events" {
		t.Errorf("Topic = %q, want 'events'", msg.Topic)
	}
	if msg.Partition != 2 {
		t.Errorf("Partition = %d, want 2", msg.Partition)
	}
	if msg.Offset != 1000 {
		t.Errorf("Offset = %d, want 1000", msg.Offset)
	}
	if string(msg.Key) != "key-1" {
		t.Errorf("Key = %q, want 'key-1'", msg.Key)
	}
	if string(msg.Value) != `{"event":"click"}` {
		t.Errorf("Value = %q, want '{\"event\":\"click\"}'", msg.Value)
	}
	if msg.Timestamp != now {
		t.Errorf("Timestamp = %v, want %v", msg.Timestamp, now)
	}
}

func TestConsumerMessageHeaders(t *testing.T) {
	headers := map[string][]byte{
		"content-type": []byte("application/json"),
		"trace-id":     []byte("trace-abc"),
		"source":       []byte("service-a"),
	}

	msg := &ConsumerMessage{
		Topic:   "events",
		Value:   []byte("data"),
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

func TestConsumerMessageNilHeaders(t *testing.T) {
	msg := &ConsumerMessage{
		Topic: "topic",
		Value: []byte("data"),
	}
	if msg.Headers != nil {
		t.Error("expected nil headers by default")
	}
}

func TestConsumerMessageEmptyHeaders(t *testing.T) {
	msg := &ConsumerMessage{
		Topic:   "topic",
		Value:   []byte("data"),
		Headers: map[string][]byte{},
	}
	if len(msg.Headers) != 0 {
		t.Errorf("expected 0 headers, got %d", len(msg.Headers))
	}
}

func TestConsumerGroupIDAndTopics(t *testing.T) {
	topics := []string{"topic-a", "topic-b", "topic-c"}
	c := &Consumer{
		groupID: "my-group",
		topics:  topics,
	}

	if c.GroupID() != "my-group" {
		t.Errorf("GroupID() = %q, want 'my-group'", c.GroupID())
	}
	got := c.Topics()
	if len(got) != 3 {
		t.Fatalf("expected 3 topics, got %d", len(got))
	}
	for i, topic := range topics {
		if got[i] != topic {
			t.Errorf("Topics()[%d] = %q, want %q", i, got[i], topic)
		}
	}
}

func TestConsumerCloseIdempotent(t *testing.T) {
	c := &Consumer{
		closed:       true,
		stopChan:     make(chan struct{}),
		messagesChan: make(chan *ConsumerMessage, 1),
		errorsChan:   make(chan error, 1),
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close on already-closed consumer should return nil, got %v", err)
	}
}

func TestConsumerStartWhenClosed(t *testing.T) {
	c := &Consumer{closed: true}
	msgCh, errCh := c.Start(context.Background())

	// Should get an error from the error channel
	select {
	case err, ok := <-errCh:
		if !ok {
			// channel closed, check if we got the error
			break
		}
		if err == nil {
			t.Error("expected non-nil error")
		} else if err.Error() != "streamline: consumer is closed" {
			t.Errorf("unexpected error: %v", err)
		}
	}

	// Message channel should be closed
	select {
	case _, ok := <-msgCh:
		if ok {
			t.Error("expected closed message channel")
		}
	default:
		// Closed channel returns immediately with zero value
	}
}

func TestConsumerPollWhenClosed(t *testing.T) {
	c := &Consumer{closed: true}
	_, err := c.Poll(context.Background(), 10, time.Second)
	if err == nil {
		t.Fatal("expected error when polling closed consumer")
	}
	if err.Error() != "streamline: consumer is closed" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestConsumerCommit(t *testing.T) {
	c := &Consumer{}
	if err := c.Commit(); err != nil {
		t.Errorf("Commit should return nil, got %v", err)
	}
}

func TestConsumerGroupHandler(t *testing.T) {
	msgCh := make(chan *ConsumerMessage, 10)
	stopCh := make(chan struct{})
	handler := &consumerGroupHandler{
		messagesChan: msgCh,
		stopChan:     stopCh,
	}

	// Setup should return nil
	if err := handler.Setup(nil); err != nil {
		t.Errorf("Setup should return nil, got %v", err)
	}

	// Cleanup should return nil
	if err := handler.Cleanup(nil); err != nil {
		t.Errorf("Cleanup should return nil, got %v", err)
	}
}

func TestConsumerPollWithContext(t *testing.T) {
	msgCh := make(chan *ConsumerMessage, 10)
	c := &Consumer{
		messagesChan: msgCh,
	}

	// Put some messages
	msgCh <- &ConsumerMessage{Topic: "t", Offset: 1, Value: []byte("m1")}
	msgCh <- &ConsumerMessage{Topic: "t", Offset: 2, Value: []byte("m2")}

	msgs, err := c.Poll(context.Background(), 5, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	if msgs[0].Offset != 1 {
		t.Errorf("msgs[0].Offset = %d, want 1", msgs[0].Offset)
	}
	if msgs[1].Offset != 2 {
		t.Errorf("msgs[1].Offset = %d, want 2", msgs[1].Offset)
	}
}

func TestConsumerPollContextCancellation(t *testing.T) {
	msgCh := make(chan *ConsumerMessage, 10)
	c := &Consumer{
		messagesChan: msgCh,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	msgs, err := c.Poll(ctx, 10, 5*time.Second)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages, got %d", len(msgs))
	}
}

func TestConsumerPollMaxRecords(t *testing.T) {
	msgCh := make(chan *ConsumerMessage, 10)
	c := &Consumer{
		messagesChan: msgCh,
	}

	// Put 5 messages but only request 3
	for i := 0; i < 5; i++ {
		msgCh <- &ConsumerMessage{Topic: "t", Offset: int64(i)}
	}

	msgs, err := c.Poll(context.Background(), 3, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
}
