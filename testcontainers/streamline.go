// Package streamline provides a Testcontainers module for Streamline.
//
// Streamline is a Kafka-compatible streaming platform - "The Redis of Streaming".
// This module allows you to easily spin up a Streamline container for integration testing.
//
// # Quick Start
//
//	container, err := streamline.RunContainer(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer container.Terminate(ctx)
//
//	bootstrapServers, err := container.BootstrapServers(ctx)
//	// Use bootstrapServers with any Kafka client
//
// # Features
//
//   - Kafka protocol compatible - use existing Kafka clients unchanged
//   - Fast startup (~100ms vs seconds for Kafka)
//   - Low memory footprint (<50MB)
//   - No ZooKeeper or KRaft required
//   - Built-in HTTP API for health checks and metrics
package streamline

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// DefaultImage is the default Docker image for Streamline
	DefaultImage = "ghcr.io/streamlinelabs/streamline"
	// DefaultTag is the default image tag
	DefaultTag = "latest"
	// KafkaPort is the Kafka protocol port
	KafkaPort = "9092/tcp"
	// HTTPPort is the HTTP API port
	HTTPPort = "9094/tcp"
)

// StreamlineContainer represents a running Streamline container
type StreamlineContainer struct {
	testcontainers.Container
}

// ContainerOption is a function that configures a Streamline container
type ContainerOption func(*testcontainers.ContainerRequest)

// WithImage sets a custom Docker image
func WithImage(image string) ContainerOption {
	return func(req *testcontainers.ContainerRequest) {
		req.Image = image
	}
}

// WithTag sets the Docker image tag
func WithTag(tag string) ContainerOption {
	return func(req *testcontainers.ContainerRequest) {
		req.Image = fmt.Sprintf("%s:%s", DefaultImage, tag)
	}
}

// WithLogLevel sets the log level (trace, debug, info, warn, error)
func WithLogLevel(level string) ContainerOption {
	return func(req *testcontainers.ContainerRequest) {
		if req.Env == nil {
			req.Env = make(map[string]string)
		}
		req.Env["STREAMLINE_LOG_LEVEL"] = level
	}
}

// WithDebugLogging enables debug logging
func WithDebugLogging() ContainerOption {
	return WithLogLevel("debug")
}

// WithTraceLogging enables trace logging
func WithTraceLogging() ContainerOption {
	return WithLogLevel("trace")
}

// RunContainer starts a new Streamline container
func RunContainer(ctx context.Context, opts ...ContainerOption) (*StreamlineContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("%s:%s", DefaultImage, DefaultTag),
		ExposedPorts: []string{KafkaPort, HTTPPort},
		Env: map[string]string{
			"STREAMLINE_LISTEN_ADDR": "0.0.0.0:9092",
			"STREAMLINE_HTTP_ADDR":   "0.0.0.0:9094",
		},
		WaitingFor: wait.ForHTTP("/health").
			WithPort("9094").
			WithStartupTimeout(30 * time.Second),
	}

	// Apply options
	for _, opt := range opts {
		opt(&req)
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start streamline container: %w", err)
	}

	return &StreamlineContainer{Container: container}, nil
}

// BootstrapServers returns the Kafka bootstrap servers connection string
func (c *StreamlineContainer) BootstrapServers(ctx context.Context) (string, error) {
	host, err := c.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get host: %w", err)
	}

	port, err := c.MappedPort(ctx, "9092")
	if err != nil {
		return "", fmt.Errorf("failed to get mapped port: %w", err)
	}

	return fmt.Sprintf("%s:%s", host, port.Port()), nil
}

// HTTPEndpoint returns the HTTP API base URL
func (c *StreamlineContainer) HTTPEndpoint(ctx context.Context) (string, error) {
	host, err := c.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get host: %w", err)
	}

	port, err := c.MappedPort(ctx, "9094")
	if err != nil {
		return "", fmt.Errorf("failed to get mapped port: %w", err)
	}

	return fmt.Sprintf("http://%s:%s", host, port.Port()), nil
}

// HealthEndpoint returns the health check endpoint URL
func (c *StreamlineContainer) HealthEndpoint(ctx context.Context) (string, error) {
	base, err := c.HTTPEndpoint(ctx)
	if err != nil {
		return "", err
	}
	return base + "/health", nil
}

// MetricsEndpoint returns the Prometheus metrics endpoint URL
func (c *StreamlineContainer) MetricsEndpoint(ctx context.Context) (string, error) {
	base, err := c.HTTPEndpoint(ctx)
	if err != nil {
		return "", err
	}
	return base + "/metrics", nil
}

// CreateTopic creates a topic with the specified name and partitions
func (c *StreamlineContainer) CreateTopic(ctx context.Context, name string, partitions int) error {
	code, _, err := c.Exec(ctx, []string{
		"streamline-cli",
		"topics", "create", name,
		"--partitions", fmt.Sprintf("%d", partitions),
	})
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("failed to create topic, exit code: %d", code)
	}
	return nil
}

// CreateTopics creates multiple topics at once
func (c *StreamlineContainer) CreateTopics(ctx context.Context, topics map[string]int) error {
	for name, partitions := range topics {
		if err := c.CreateTopic(ctx, name, partitions); err != nil {
			return fmt.Errorf("failed to create topic %s: %w", name, err)
		}
	}
	return nil
}

// WaitForTopics waits until all specified topics exist (with timeout)
func (c *StreamlineContainer) WaitForTopics(ctx context.Context, topics []string, timeout time.Duration) error {
	deadline := time.After(timeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return fmt.Errorf("timeout waiting for topics after %v", timeout)
		case <-ticker.C:
			allExist := true
			for _, topic := range topics {
				code, _, err := c.Exec(ctx, []string{
					"streamline-cli", "topics", "describe", topic,
				})
				if err != nil || code != 0 {
					allExist = false
					break
				}
			}
			if allExist {
				return nil
			}
		}
	}
}

// ProduceMessage produces a single message to a topic
func (c *StreamlineContainer) ProduceMessage(ctx context.Context, topic string, value string) error {
	code, _, err := c.Exec(ctx, []string{
		"streamline-cli", "produce", topic, "-m", value,
	})
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("failed to produce message, exit code: %d", code)
	}
	return nil
}

// ProduceKeyedMessage produces a message with a key to a topic
func (c *StreamlineContainer) ProduceKeyedMessage(ctx context.Context, topic, key, value string) error {
	code, _, err := c.Exec(ctx, []string{
		"streamline-cli", "produce", topic, "-m", value, "-k", key,
	})
	if err != nil {
		return fmt.Errorf("failed to produce keyed message: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("failed to produce keyed message, exit code: %d", code)
	}
	return nil
}

// WithTopics creates topics when the container starts
func WithTopics(topics map[string]int) ContainerOption {
	return func(req *testcontainers.ContainerRequest) {
		// Topics will be created after the container starts via LifecycleHooks
		// Store them as env var for post-start processing
		for name, partitions := range topics {
			if req.Env == nil {
				req.Env = make(map[string]string)
			}
			req.Env[fmt.Sprintf("STREAMLINE_AUTO_TOPIC_%s", name)] = fmt.Sprintf("%d", partitions)
		}
	}
}

// WithPlayground enables playground mode (pre-loaded demo topics)
func WithPlayground() ContainerOption {
	return func(req *testcontainers.ContainerRequest) {
		if req.Cmd == nil {
			req.Cmd = []string{"--playground"}
		} else {
			req.Cmd = append(req.Cmd, "--playground")
		}
	}
}

// WithInMemory enables in-memory storage mode (no disk persistence)
func WithInMemory() ContainerOption {
	return func(req *testcontainers.ContainerRequest) {
		if req.Cmd == nil {
			req.Cmd = []string{"--in-memory"}
		} else {
			req.Cmd = append(req.Cmd, "--in-memory")
		}
	}
}

// InfoEndpoint returns the server info endpoint URL
func (c *StreamlineContainer) InfoEndpoint(ctx context.Context) (string, error) {
	base, err := c.HTTPEndpoint(ctx)
	if err != nil {
		return "", err
	}
	return base + "/info", nil
}
