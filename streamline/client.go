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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Config holds the configuration for a Streamline client.
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
	// 0 = no acks, 1 = leader only, -1 = all replicas.
	RequiredAcks int16

	// Compression specifies the compression codec.
	// 0 = none, 1 = gzip, 2 = snappy, 3 = lz4, 4 = zstd.
	Compression int8

	// BatchSize is the number of messages to batch together.
	BatchSize int

	// BatchTimeout is how long to wait before flushing a partial batch.
	BatchTimeout time.Duration

	// Idempotent enables exactly-once semantics.
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

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Brokers:                 []string{"localhost:9092"},
		ClientID:                "streamline-go-client",
		Version:                 sarama.V2_8_0_0,
		ConnectionTimeout:       10 * time.Second,
		MetadataRefreshInterval: 5 * time.Minute,
		Producer: ProducerConfig{
			MaxMessageBytes: 1048576, // 1MB
			RequiredAcks:    -1,      // All replicas
			Compression:     0,       // None
			BatchSize:       16384,
			BatchTimeout:    10 * time.Millisecond,
			Idempotent:      false,
			Retries:         3,
		},
		Consumer: ConsumerConfig{
			AutoOffsetReset:   "latest",
			SessionTimeout:    30 * time.Second,
			HeartbeatInterval: 3 * time.Second,
			MaxPollRecords:    500,
			IsolationLevel:    0, // Read uncommitted
		},
	}
}

// Client is the main entry point for interacting with Streamline.
type Client struct {
	config       Config
	saramaConfig *sarama.Config
	client       sarama.Client

	Producer *Producer
	Admin    *Admin

	mu     sync.RWMutex
	closed bool
}

// NewClient creates a new Streamline client.
func NewClient(config Config) (*Client, error) {
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("streamline: at least one broker address is required")
	}

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

	// Create sarama client
	client, err := sarama.NewClient(config.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("streamline: failed to create client: %w", err)
	}

	c := &Client{
		config:       config,
		saramaConfig: saramaConfig,
		client:       client,
	}

	// Initialize producer
	c.Producer, err = newProducer(client, saramaConfig)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("streamline: failed to create producer: %w", err)
	}

	// Initialize admin
	c.Admin, err = newAdmin(client)
	if err != nil {
		c.Producer.Close()
		client.Close()
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

	return newConsumer(c.client, c.saramaConfig, groupID, topics)
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
