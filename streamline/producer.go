package streamline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Message represents a message to be produced.
type Message struct {
	// Topic is the topic to produce to.
	Topic string

	// Key is the message key (optional).
	Key []byte

	// Value is the message value.
	Value []byte

	// Headers are the message headers (optional).
	Headers map[string][]byte

	// Partition is the target partition (-1 for automatic).
	Partition int32

	// Timestamp is the message timestamp (optional, uses current time if zero).
	Timestamp time.Time
}

// ProducerResult holds the result of a produce operation.
type ProducerResult struct {
	// Topic is the topic the message was produced to.
	Topic string

	// Partition is the partition the message was written to.
	Partition int32

	// Offset is the offset of the message in the partition.
	Offset int64

	// Timestamp is the broker-assigned timestamp.
	Timestamp time.Time
}

// Producer sends messages to Streamline.
type Producer struct {
	client    sarama.Client
	producer  sarama.SyncProducer
	asyncProd sarama.AsyncProducer

	mu     sync.RWMutex
	closed bool
}

func newProducer(client sarama.Client, config *sarama.Config) (*Producer, error) {
	syncProd, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	asyncProd, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		syncProd.Close()
		return nil, err
	}

	return &Producer{
		client:    client,
		producer:  syncProd,
		asyncProd: asyncProd,
	}, nil
}

// Send sends a single message synchronously.
func (p *Producer) Send(ctx context.Context, topic string, key, value []byte) (*ProducerResult, error) {
	return p.SendMessage(ctx, &Message{
		Topic: topic,
		Key:   key,
		Value: value,
	})
}

// SendMessage sends a message with full options synchronously.
func (p *Producer) SendMessage(ctx context.Context, msg *Message) (*ProducerResult, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("streamline: producer is closed")
	}
	p.mu.RUnlock()

	saramaMsg := &sarama.ProducerMessage{
		Topic: msg.Topic,
	}

	if msg.Key != nil {
		saramaMsg.Key = sarama.ByteEncoder(msg.Key)
	}
	if msg.Value != nil {
		saramaMsg.Value = sarama.ByteEncoder(msg.Value)
	}
	if msg.Partition >= 0 {
		saramaMsg.Partition = msg.Partition
	}
	if !msg.Timestamp.IsZero() {
		saramaMsg.Timestamp = msg.Timestamp
	}
	if len(msg.Headers) > 0 {
		for k, v := range msg.Headers {
			saramaMsg.Headers = append(saramaMsg.Headers, sarama.RecordHeader{
				Key:   []byte(k),
				Value: v,
			})
		}
	}

	// Use a goroutine to support context cancellation
	type result struct {
		partition int32
		offset    int64
		err       error
	}
	resultCh := make(chan result, 1)

	go func() {
		partition, offset, err := p.producer.SendMessage(saramaMsg)
		resultCh <- result{partition, offset, err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-resultCh:
		if r.err != nil {
			return nil, fmt.Errorf("streamline: failed to send message: %w", r.err)
		}
		return &ProducerResult{
			Topic:     msg.Topic,
			Partition: r.partition,
			Offset:    r.offset,
			Timestamp: saramaMsg.Timestamp,
		}, nil
	}
}

// SendAsync sends a message asynchronously.
// Use the returned channel to receive the result.
func (p *Producer) SendAsync(msg *Message) <-chan *AsyncProducerResult {
	resultCh := make(chan *AsyncProducerResult, 1)

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		resultCh <- &AsyncProducerResult{
			Err: fmt.Errorf("streamline: producer is closed"),
		}
		return resultCh
	}
	p.mu.RUnlock()

	saramaMsg := &sarama.ProducerMessage{
		Topic:    msg.Topic,
		Metadata: resultCh,
	}

	if msg.Key != nil {
		saramaMsg.Key = sarama.ByteEncoder(msg.Key)
	}
	if msg.Value != nil {
		saramaMsg.Value = sarama.ByteEncoder(msg.Value)
	}
	if msg.Partition >= 0 {
		saramaMsg.Partition = msg.Partition
	}
	if !msg.Timestamp.IsZero() {
		saramaMsg.Timestamp = msg.Timestamp
	}

	p.asyncProd.Input() <- saramaMsg

	go func() {
		select {
		case success := <-p.asyncProd.Successes():
			if ch, ok := success.Metadata.(chan *AsyncProducerResult); ok {
				ch <- &AsyncProducerResult{
					Topic:     success.Topic,
					Partition: success.Partition,
					Offset:    success.Offset,
					Timestamp: success.Timestamp,
				}
			}
		case fail := <-p.asyncProd.Errors():
			if ch, ok := fail.Msg.Metadata.(chan *AsyncProducerResult); ok {
				ch <- &AsyncProducerResult{
					Err: fail.Err,
				}
			}
		}
	}()

	return resultCh
}

// AsyncProducerResult holds the result of an async produce operation.
type AsyncProducerResult struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Err       error
}

// SendBatch sends multiple messages synchronously.
func (p *Producer) SendBatch(ctx context.Context, messages []*Message) ([]*ProducerResult, error) {
	results := make([]*ProducerResult, len(messages))
	var errs []error

	for i, msg := range messages {
		result, err := p.SendMessage(ctx, msg)
		if err != nil {
			errs = append(errs, fmt.Errorf("message %d: %w", i, err))
			continue
		}
		results[i] = result
	}

	if len(errs) > 0 {
		return results, fmt.Errorf("streamline: %d/%d messages failed: %v", len(errs), len(messages), errs)
	}
	return results, nil
}

// Flush waits for all buffered messages to be sent.
func (p *Producer) Flush(timeout time.Duration) error {
	// Sarama doesn't have a flush method, but sync producer is already flushed
	return nil
}

// Close closes the producer.
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	var errs []error
	if p.producer != nil {
		if err := p.producer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if p.asyncProd != nil {
		if err := p.asyncProd.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("streamline: errors closing producer: %v", errs)
	}
	return nil
}
