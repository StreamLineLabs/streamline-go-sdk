package streamline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// ConsumerMessage represents a consumed message.
type ConsumerMessage struct {
	// Topic is the topic the message was consumed from.
	Topic string

	// Partition is the partition the message was consumed from.
	Partition int32

	// Offset is the offset of the message.
	Offset int64

	// Key is the message key.
	Key []byte

	// Value is the message value.
	Value []byte

	// Headers are the message headers.
	Headers map[string][]byte

	// Timestamp is the message timestamp.
	Timestamp time.Time
}

// Consumer consumes messages from Streamline using consumer groups.
type Consumer struct {
	client         sarama.Client
	config         *sarama.Config
	groupID        string
	topics         []string
	consumerGrp    sarama.ConsumerGroup
	handler        *consumerGroupHandler
	messagesChan   chan *ConsumerMessage
	errorsChan     chan error
	circuitBreaker *CircuitBreaker

	mu       sync.RWMutex
	closed   bool
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func newConsumer(client sarama.Client, config *sarama.Config, groupID string, topics []string, cb *CircuitBreaker) (*Consumer, error) {
	consumerGrp, err := sarama.NewConsumerGroupFromClient(groupID, client)
	if err != nil {
		return nil, err
	}

	messagesChan := make(chan *ConsumerMessage, 100)
	errorsChan := make(chan error, 10)
	stopChan := make(chan struct{})

	handler := &consumerGroupHandler{
		messagesChan: messagesChan,
		stopChan:     stopChan,
	}

	c := &Consumer{
		client:         client,
		config:         config,
		groupID:        groupID,
		topics:         topics,
		consumerGrp:    consumerGrp,
		handler:        handler,
		messagesChan:   messagesChan,
		errorsChan:     errorsChan,
		stopChan:       stopChan,
		circuitBreaker: cb,
	}

	return c, nil
}

// Start begins consuming messages.
// Returns a channel of messages and a channel of errors.
func (c *Consumer) Start(ctx context.Context) (<-chan *ConsumerMessage, <-chan error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		errCh := make(chan error, 1)
		errCh <- fmt.Errorf("streamline: consumer is closed")
		close(errCh)
		msgCh := make(chan *ConsumerMessage)
		close(msgCh)
		return msgCh, errCh
	}
	c.mu.Unlock()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopChan:
				return
			default:
				if c.circuitBreaker != nil && !c.circuitBreaker.Allow() {
					select {
					case c.errorsChan <- ErrCircuitOpen:
					default:
					}
					time.Sleep(100 * time.Millisecond)
					continue
				}
				if err := c.consumerGrp.Consume(ctx, c.topics, c.handler); err != nil {
					if c.circuitBreaker != nil && IsRetryable(err) {
						c.circuitBreaker.RecordFailure()
					}
					select {
					case c.errorsChan <- err:
					default:
					}
				} else if c.circuitBreaker != nil {
					c.circuitBreaker.RecordSuccess()
				}
			}
		}
	}()

	// Forward consumer group errors
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopChan:
				return
			case err, ok := <-c.consumerGrp.Errors():
				if !ok {
					return
				}
				select {
				case c.errorsChan <- err:
				default:
				}
			}
		}
	}()

	return c.messagesChan, c.errorsChan
}

// Poll returns up to maxRecords messages, waiting up to timeout.
func (c *Consumer) Poll(ctx context.Context, maxRecords int, timeout time.Duration) ([]*ConsumerMessage, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, fmt.Errorf("streamline: consumer is closed")
	}
	c.mu.RUnlock()

	messages := make([]*ConsumerMessage, 0, maxRecords)
	timeoutCh := time.After(timeout)

	for len(messages) < maxRecords {
		select {
		case <-ctx.Done():
			return messages, ctx.Err()
		case <-timeoutCh:
			return messages, nil
		case msg, ok := <-c.messagesChan:
			if !ok {
				return messages, nil
			}
			messages = append(messages, msg)
		}
	}

	return messages, nil
}

// Commit commits the offsets for the consumed messages.
// This forces an immediate offset commit for all messages that have been
// marked via MarkMessage (which happens automatically when messages are
// delivered through the messages channel).
func (c *Consumer) Commit() error {
	c.handler.mu.Lock()
	session := c.handler.session
	c.handler.mu.Unlock()

	if session == nil {
		return &StreamlineError{
			Code:      ErrConnection,
			Message:   "no active consumer group session",
			Hint:      "Ensure the consumer is started and has joined the group before committing.",
			Retryable: true,
		}
	}

	session.Commit()
	return nil
}

// GroupID returns the consumer group ID.
func (c *Consumer) GroupID() string {
	return c.groupID
}

// Topics returns the subscribed topics.
func (c *Consumer) Topics() []string {
	return c.topics
}

// Close closes the consumer.
func (c *Consumer) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.stopChan)
	c.mu.Unlock()

	c.wg.Wait()

	var errs []error
	if c.consumerGrp != nil {
		if err := c.consumerGrp.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	close(c.messagesChan)
	close(c.errorsChan)

	if len(errs) > 0 {
		return fmt.Errorf("streamline: errors closing consumer: %v", errs)
	}
	return nil
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler.
type consumerGroupHandler struct {
	messagesChan chan *ConsumerMessage
	stopChan     chan struct{}
	mu           sync.Mutex
	session      sarama.ConsumerGroupSession
}

func (h *consumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.mu.Lock()
	h.session = session
	h.mu.Unlock()
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	h.mu.Lock()
	h.session = nil
	h.mu.Unlock()
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-h.stopChan:
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			headers := make(map[string][]byte)
			for _, h := range msg.Headers {
				headers[string(h.Key)] = h.Value
			}

			consumerMsg := &ConsumerMessage{
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Key:       msg.Key,
				Value:     msg.Value,
				Headers:   headers,
				Timestamp: msg.Timestamp,
			}

			select {
			case h.messagesChan <- consumerMsg:
				session.MarkMessage(msg, "")
			case <-h.stopChan:
				return nil
			}
		}
	}
}

