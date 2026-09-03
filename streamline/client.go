// Package streamline provides an idiomatic Go client for Streamline,
// a Kafka-compatible streaming platform.
//
// Example usage:
//
//	client, err := streamline.NewClient(streamline.Config{
//	    Brokers: []string{"localhost:9092"},
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	// Produce a message
//	result, err := client.Producer.Send(ctx, "my-topic", nil, []byte("Hello, World!"))
//	if err != nil {
//	    log.Fatal(err)
//	}
//	log.Printf("produced to partition %d at offset %d", result.Partition, result.Offset)
package streamline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Client is the main entry point for interacting with Streamline.
type Client struct {
	config         Config
	saramaConfig   *sarama.Config
	client         sarama.Client
	circuitBreaker *CircuitBreaker

	Producer *Producer
	Admin    *Admin
	Metrics  *ClientMetrics

	mu     sync.RWMutex
	closed bool
}

// NewClient creates a new Streamline client.
//
// Invalid zero-valued Config fields receive safe defaults. Meaningful producer
// zero values retain their Kafka semantics. Start from DefaultConfig to use all
// recommended SDK defaults.
func NewClient(config Config) (*Client, error) {
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("streamline: at least one broker address is required")
	}

	config = config.withDefaults()

	saramaConfig, err := buildSaramaConfig(config)
	if err != nil {
		return nil, err
	}

	// Create sarama client
	client, err := sarama.NewClient(config.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("streamline: failed to create client: %w", err)
	}

	c := &Client{
		config:       config,
		saramaConfig: saramaConfig,
		client:       client,
		Metrics:      NewClientMetrics(),
	}

	if config.CircuitBreaker != nil {
		c.circuitBreaker = NewCircuitBreaker(*config.CircuitBreaker)
	}

	// Initialize producer
	c.Producer, err = newProducer(client, c.circuitBreaker)
	if err != nil {
		if closeErr := client.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		return nil, fmt.Errorf("streamline: failed to create producer: %w", err)
	}

	// Initialize admin
	c.Admin, err = newAdmin(client)
	if err != nil {
		if closeErr := c.Producer.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		if closeErr := client.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		return nil, fmt.Errorf("streamline: failed to create admin client: %w", err)
	}

	return c, nil
}

// NewConsumer creates a new consumer for consuming messages.
func (c *Client) NewConsumer(ctx context.Context, groupID string, topics []string) (*Consumer, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, fmt.Errorf("streamline: client is closed")
	}
	c.mu.RUnlock()

	for _, topic := range topics {
		if err := validateTopicName(topic); err != nil {
			return nil, fmt.Errorf("streamline: %w", err)
		}
	}

	return newConsumer(c.client, c.saramaConfig, groupID, topics, c.circuitBreaker, c.config.HTTPEndpoint)
}

// Brokers returns the list of configured brokers.
func (c *Client) Brokers() []string {
	return c.config.Brokers
}

// Close closes all resources held by the client.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	var errs []error

	if c.Producer != nil {
		if err := c.Producer.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if c.Admin != nil {
		if err := c.Admin.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if c.client != nil {
		if err := c.client.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("streamline: errors closing client: %v", errs)
	}
	return nil
}

// httpEndpoint returns the configured HTTP endpoint, falling back to the default.
func (c *Client) httpEndpoint() string {
	if c.config.HTTPEndpoint != "" {
		return c.config.HTTPEndpoint
	}
	return defaultHTTPEndpoint
}

// ── Moonshot: Agent Memory (M1) ────────────────────────────────────

// MemoryEntry is a memory record to store via the memory API.
type MemoryEntry struct {
	AgentID    string   `json:"agent_id"`
	Content    string   `json:"content"`
	Kind       string   `json:"kind"` // "fact", "observation", "preference", "procedure"
	Importance float64  `json:"importance"`
	Tags       []string `json:"tags,omitempty"`
	Namespace  string   `json:"namespace,omitempty"` // shared namespace for multi-agent memory
}

// MemoryQuery is a semantic recall query against agent memory.
type MemoryQuery struct {
	AgentID   string `json:"agent_id"`
	Query     string `json:"query"`
	K         int    `json:"k"`
	Namespace string `json:"namespace,omitempty"`
}

// MemoryHit is a single result from a memory recall query.
type MemoryHit struct {
	Tier    string  `json:"tier"`
	Content string  `json:"content"`
	Score   float64 `json:"score"`
	Topic   string  `json:"topic"`
	Offset  int64   `json:"offset"`
}

// MemoryRemember stores a memory entry via the Streamline HTTP memory API.
func (c *Client) MemoryRemember(ctx context.Context, entry MemoryEntry) (err error) {
	endpoint := c.httpEndpoint()

	payload, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("streamline: failed to marshal memory entry: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/memory/remember", endpoint)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("streamline: failed to create remember request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("streamline: remember request failed: %w", err)
	}
	defer func() { err = joinClose(err, resp.Body, "close remember response body") }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("streamline: remember failed (HTTP %d) and response body could not be read: %w", resp.StatusCode, readErr)
		}
		return fmt.Errorf("streamline: remember failed (HTTP %d): %s", resp.StatusCode, string(body))
	}
	return nil
}

// MemoryRecall retrieves memories by semantic similarity via the Streamline HTTP memory API.
func (c *Client) MemoryRecall(ctx context.Context, query MemoryQuery) (_ []MemoryHit, err error) {
	endpoint := c.httpEndpoint()

	if query.K == 0 {
		query.K = 10
	}

	payload, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("streamline: failed to marshal recall query: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/memory/recall", endpoint)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("streamline: failed to create recall request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("streamline: recall request failed: %w", err)
	}
	defer func() { err = joinClose(err, resp.Body, "close recall response body") }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("streamline: failed to read recall response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("streamline: recall failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	var result struct {
		Hits []MemoryHit `json:"hits"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("streamline: failed to parse recall response: %w", err)
	}
	return result.Hits, nil
}
