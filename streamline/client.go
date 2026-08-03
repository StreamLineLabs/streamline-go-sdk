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
//	err = client.Producer.Send(ctx, "my-topic", nil, []byte("Hello, World!"))
package streamline

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Config holds the configuration for a Streamline client.
//
// A zero value is usable for fields whose zero value is invalid (for example
// connection timeouts). Producer fields where zero has Kafka semantics
// (RequiredAcks, BatchSize, BatchTimeout, Retries) are preserved. Start from
// DefaultConfig when the SDK's recommended producer defaults are desired.
// Brokers has no default — at least one address is required.
type Config struct {
	// Brokers is a list of broker addresses.
	Brokers []string

	// ClientID identifies this client to the server.
	ClientID string

	// Version specifies the Kafka protocol version to use.
	Version sarama.KafkaVersion

	// SASL configuration for authentication.
	SASL *SASLConfig

	// TLS configuration for secure connections.
	TLS *TLSConfig

	// Producer configuration.
	Producer ProducerConfig

	// Consumer configuration.
	Consumer ConsumerConfig

	// ConnectionTimeout is the timeout for connecting to brokers.
	ConnectionTimeout time.Duration

	// MetadataRefreshInterval is how often to refresh cluster metadata.
	MetadataRefreshInterval time.Duration

	// CircuitBreaker configures the circuit breaker. Nil means disabled (opt-in).
	CircuitBreaker *CircuitBreakerConfig

	// HTTPEndpoint is the base URL for the Streamline HTTP admin API (port 9094).
	// Used by moonshot features (semantic search, memory, branches, attestation).
	// Default: "http://localhost:9094"
	HTTPEndpoint string
}

// SASLConfig holds SASL authentication configuration.
type SASLConfig struct {
	// Mechanism is the SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512).
	Mechanism string

	// Username for authentication.
	Username string

	// Password for authentication.
	Password string
}

// TLSConfig holds TLS configuration.
type TLSConfig struct {
	// Enable TLS.
	Enable bool

	// CertFile is the path to the client certificate.
	CertFile string

	// KeyFile is the path to the client key.
	KeyFile string

	// CAFile is the path to the CA certificate.
	CAFile string

	// InsecureSkipVerify disables certificate verification.
	InsecureSkipVerify bool
}

// ProducerConfig holds producer-specific configuration.
type ProducerConfig struct {
	// MaxMessageBytes is the maximum message size.
	MaxMessageBytes int

	// RequiredAcks specifies the acknowledgment level.
	// 0 = no response, 1 = leader only, -1 = all replicas.
	RequiredAcks int16

	// Compression specifies the compression codec.
	// 0 = none, 1 = gzip, 2 = snappy, 3 = lz4, 4 = zstd.
	Compression int8

	// BatchSize is the number of messages to batch together.
	BatchSize int

	// BatchTimeout is how long to wait before flushing a partial batch.
	BatchTimeout time.Duration

	// Idempotent enables exactly-once semantics. It requires RequiredAcks to be
	// -1 (the default).
	Idempotent bool

	// Retries is the number of times to retry failed sends.
	Retries int
}

// ConsumerConfig holds consumer-specific configuration.
type ConsumerConfig struct {
	// GroupID is the consumer group ID.
	GroupID string

	// AutoOffsetReset specifies where to start consuming.
	// "earliest" or "latest".
	AutoOffsetReset string

	// SessionTimeout is the timeout for consumer group sessions.
	SessionTimeout time.Duration

	// HeartbeatInterval is how often to send heartbeats.
	HeartbeatInterval time.Duration

	// MaxPollRecords is the maximum records per poll.
	MaxPollRecords int

	// IsolationLevel specifies the transaction isolation level.
	// 0 = read_uncommitted, 1 = read_committed.
	IsolationLevel int8
}

// Default configuration values. DefaultConfig applies all of them;
// Config.withDefaults applies only defaults whose zero value is not itself a
// meaningful Kafka setting.
const (
	defaultBroker                  = "localhost:9092"
	defaultClientID                = "streamline-go-client"
	defaultConnectionTimeout       = 10 * time.Second
	defaultMetadataRefreshInterval = 5 * time.Minute
	defaultHTTPEndpoint            = "http://localhost:9094"

	defaultMaxMessageBytes = 1048576 // 1MB
	defaultRequiredAcks    = int16(-1)
	defaultBatchSize       = 16384
	defaultBatchTimeout    = 10 * time.Millisecond
	defaultRetries         = 3

	defaultAutoOffsetReset   = "latest"
	defaultSessionTimeout    = 30 * time.Second
	defaultHeartbeatInterval = 3 * time.Second
	defaultMaxPollRecords    = 500

	// minHeartbeatInterval is the smallest heartbeat interval accepted by the
	// Kafka protocol implementation.
	minHeartbeatInterval = time.Millisecond
)

// defaultKafkaVersion is the protocol version negotiated when Config.Version is unset.
var defaultKafkaVersion = sarama.V2_8_0_0

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Brokers:                 []string{defaultBroker},
		ClientID:                defaultClientID,
		Version:                 defaultKafkaVersion,
		ConnectionTimeout:       defaultConnectionTimeout,
		MetadataRefreshInterval: defaultMetadataRefreshInterval,
		HTTPEndpoint:            defaultHTTPEndpoint,
		Producer: ProducerConfig{
			MaxMessageBytes: defaultMaxMessageBytes,
			RequiredAcks:    defaultRequiredAcks,
			Compression:     0, // None
			BatchSize:       defaultBatchSize,
			BatchTimeout:    defaultBatchTimeout,
			Idempotent:      false,
			Retries:         defaultRetries,
		},
		Consumer: ConsumerConfig{
			AutoOffsetReset:   defaultAutoOffsetReset,
			SessionTimeout:    defaultSessionTimeout,
			HeartbeatInterval: defaultHeartbeatInterval,
			MaxPollRecords:    defaultMaxPollRecords,
			IsolationLevel:    0, // Read uncommitted
		},
	}
}

// withDefaults returns a copy of the config with invalid zero-valued fields
// replaced by safe defaults. Meaningful Kafka zero values are retained.
// Brokers is deliberately not defaulted: NewClient requires at least one
// broker address.
func (c Config) withDefaults() Config {
	if c.ClientID == "" {
		c.ClientID = defaultClientID
	}
	if !c.Version.IsAtLeast(sarama.MinVersion) {
		c.Version = defaultKafkaVersion
	}
	if c.ConnectionTimeout <= 0 {
		c.ConnectionTimeout = defaultConnectionTimeout
	}
	if c.MetadataRefreshInterval <= 0 {
		c.MetadataRefreshInterval = defaultMetadataRefreshInterval
	}
	if c.HTTPEndpoint == "" {
		c.HTTPEndpoint = defaultHTTPEndpoint
	}

	if c.Producer.MaxMessageBytes <= 0 {
		c.Producer.MaxMessageBytes = defaultMaxMessageBytes
	}
	if c.Producer.Idempotent && c.Producer.RequiredAcks == 0 {
		c.Producer.RequiredAcks = defaultRequiredAcks
	}
	if c.Producer.Idempotent && c.Producer.Retries <= 0 {
		c.Producer.Retries = defaultRetries
	}

	if c.Consumer.AutoOffsetReset == "" {
		c.Consumer.AutoOffsetReset = defaultAutoOffsetReset
	}
	if c.Consumer.SessionTimeout <= 0 {
		c.Consumer.SessionTimeout = defaultSessionTimeout
	}
	if c.Consumer.HeartbeatInterval <= 0 {
		// The heartbeat interval must stay below the session timeout, so a
		// caller-supplied short session timeout shrinks the default heartbeat.
		heartbeat := defaultHeartbeatInterval
		if heartbeat >= c.Consumer.SessionTimeout {
			heartbeat = c.Consumer.SessionTimeout / 3
		}
		if heartbeat < minHeartbeatInterval {
			heartbeat = minHeartbeatInterval
		}
		c.Consumer.HeartbeatInterval = heartbeat
	}
	if c.Consumer.MaxPollRecords <= 0 {
		c.Consumer.MaxPollRecords = defaultMaxPollRecords
	}

	return c
}

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

// buildSaramaConfig translates a Streamline Config into a Sarama config.
// The config is expected to have defaults applied already (see withDefaults).
func buildSaramaConfig(config Config) (*sarama.Config, error) {
	saramaConfig := sarama.NewConfig()

	// Basic configuration
	saramaConfig.ClientID = config.ClientID
	saramaConfig.Version = config.Version
	saramaConfig.Net.DialTimeout = config.ConnectionTimeout
	saramaConfig.Metadata.RefreshFrequency = config.MetadataRefreshInterval

	// Producer configuration
	saramaConfig.Producer.MaxMessageBytes = config.Producer.MaxMessageBytes
	saramaConfig.Producer.RequiredAcks = sarama.RequiredAcks(config.Producer.RequiredAcks)
	saramaConfig.Producer.Compression = sarama.CompressionCodec(config.Producer.Compression)
	saramaConfig.Producer.Flush.Bytes = config.Producer.BatchSize
	saramaConfig.Producer.Flush.Frequency = config.Producer.BatchTimeout
	saramaConfig.Producer.Idempotent = config.Producer.Idempotent
	saramaConfig.Producer.Retry.Max = config.Producer.Retries
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true

	// Idempotent produces require full acknowledgement and a single in-flight
	// request per broker connection.
	if config.Producer.Idempotent {
		if config.Producer.RequiredAcks != defaultRequiredAcks {
			return nil, &StreamlineError{
				Code:    ErrConfiguration,
				Message: "idempotent producer requires Producer.RequiredAcks to be -1 (all replicas)",
				Hint:    "Set Producer.RequiredAcks to -1, or leave it unset to use the default",
			}
		}
		saramaConfig.Net.MaxOpenRequests = 1
	}

	// Consumer configuration
	if config.Consumer.AutoOffsetReset == "earliest" {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	saramaConfig.Consumer.Group.Session.Timeout = config.Consumer.SessionTimeout
	saramaConfig.Consumer.Group.Heartbeat.Interval = config.Consumer.HeartbeatInterval
	saramaConfig.Consumer.MaxProcessingTime = 100 * time.Millisecond
	saramaConfig.Consumer.IsolationLevel = sarama.IsolationLevel(config.Consumer.IsolationLevel)

	// SASL configuration
	if config.SASL != nil {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = config.SASL.Username
		saramaConfig.Net.SASL.Password = config.SASL.Password

		switch config.SASL.Mechanism {
		case "PLAIN":
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "SCRAM-SHA-256":
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
			}
		case "SCRAM-SHA-512":
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
			}
		default:
			return nil, fmt.Errorf("streamline: unsupported SASL mechanism: %s", config.SASL.Mechanism)
		}
	}

	if err := validateTLSConfig(config.TLS); err != nil {
		return nil, err
	}

	// TLS configuration
	if config.TLS != nil && config.TLS.Enable {
		tlsConfig, err := buildTLSConfig(config.TLS)
		if err != nil {
			return nil, err
		}
		saramaConfig.Net.TLS.Enable = true
		saramaConfig.Net.TLS.Config = tlsConfig
	}

	return saramaConfig, nil
}

func buildTLSConfig(config *TLSConfig) (*tls.Config, error) {
	if err := validateTLSConfig(config); err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: config.InsecureSkipVerify, //nolint:gosec // Explicit SDK option.
	}

	if config.CAFile != "" {
		caBytes, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("streamline: read TLS CA certificate: %w", err)
		}
		rootCAs, err := x509.SystemCertPool()
		if err != nil {
			rootCAs = x509.NewCertPool()
		}
		if !rootCAs.AppendCertsFromPEM(caBytes) {
			return nil, &StreamlineError{
				Code:    ErrConfiguration,
				Message: fmt.Sprintf("TLS CA certificate is not valid PEM: %s", config.CAFile),
			}
		}
		tlsConfig.RootCAs = rootCAs
	}

	if config.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("streamline: load TLS client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}

	return tlsConfig, nil
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
