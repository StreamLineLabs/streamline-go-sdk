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
	"io"
	"net/http"
	"strings"
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

// -------------------------------------------------------------------------
// Enhanced capabilities: batch produce, consumer groups, migration helpers
// -------------------------------------------------------------------------

// ProduceMessages produces a batch of messages to a topic
func (c *StreamlineContainer) ProduceMessages(ctx context.Context, topic string, messages []string) error {
	for _, msg := range messages {
		if err := c.ProduceMessage(ctx, topic, msg); err != nil {
			return err
		}
	}
	return nil
}

// ProduceKeyedMessages produces a batch of keyed messages to a topic
func (c *StreamlineContainer) ProduceKeyedMessages(ctx context.Context, topic string, messages map[string]string) error {
	for key, value := range messages {
		if err := c.ProduceKeyedMessage(ctx, topic, key, value); err != nil {
			return err
		}
	}
	return nil
}

// ListConsumerGroups lists consumer groups
func (c *StreamlineContainer) ListConsumerGroups(ctx context.Context) ([]string, error) {
	code, reader, err := c.Exec(ctx, []string{
		"streamline-cli", "groups", "list", "--format", "json",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}
	if code != 0 {
		return nil, fmt.Errorf("failed to list consumer groups, exit code: %d", code)
	}
	buf := make([]byte, 4096)
	n, _ := reader.Read(buf)
	output := string(buf[:n])
	var groups []string
	for _, line := range splitLines(output) {
		trimmed := trimJSON(line)
		if trimmed != "" {
			groups = append(groups, trimmed)
		}
	}
	return groups, nil
}

// AssertConsumerGroupExists asserts that a consumer group exists
func (c *StreamlineContainer) AssertConsumerGroupExists(ctx context.Context, groupID string) error {
	code, _, err := c.Exec(ctx, []string{
		"streamline-cli", "groups", "describe", groupID,
	})
	if err != nil {
		return fmt.Errorf("failed to check consumer group: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("consumer group '%s' does not exist", groupID)
	}
	return nil
}

// GetClusterInfo retrieves cluster information from the HTTP API
func (c *StreamlineContainer) GetClusterInfo(ctx context.Context) (string, error) {
	endpoint, err := c.InfoEndpoint(ctx)
	if err != nil {
		return "", err
	}
	resp, err := httpGet(ctx, endpoint)
	if err != nil {
		return "", fmt.Errorf("failed to get cluster info: %w", err)
	}
	return resp, nil
}

// AssertTopicExists asserts that a topic exists
func (c *StreamlineContainer) AssertTopicExists(ctx context.Context, topic string) error {
	code, _, err := c.Exec(ctx, []string{
		"streamline-cli", "topics", "describe", topic,
	})
	if err != nil {
		return fmt.Errorf("failed to check topic: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("topic '%s' does not exist", topic)
	}
	return nil
}

// AssertHealthy asserts the container is healthy via the health endpoint
func (c *StreamlineContainer) AssertHealthy(ctx context.Context) error {
	endpoint, err := c.HealthEndpoint(ctx)
	if err != nil {
		return err
	}
	_, err = httpGet(ctx, endpoint)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	return nil
}

// WithAuthentication enables SASL/PLAIN authentication
func WithAuthentication(username, password string) ContainerOption {
	return func(req *testcontainers.ContainerRequest) {
		if req.Env == nil {
			req.Env = make(map[string]string)
		}
		req.Env["STREAMLINE_AUTH_ENABLED"] = "true"
		req.Env["STREAMLINE_AUTH_DEFAULT_USER"] = username
		req.Env["STREAMLINE_AUTH_DEFAULT_PASSWORD"] = password
	}
}

// WithAutoCreateTopics enables auto-topic creation with a default partition count
func WithAutoCreateTopics(defaultPartitions int) ContainerOption {
	return func(req *testcontainers.ContainerRequest) {
		if req.Env == nil {
			req.Env = make(map[string]string)
		}
		req.Env["STREAMLINE_AUTO_CREATE_TOPICS"] = "true"
		req.Env["STREAMLINE_DEFAULT_PARTITIONS"] = fmt.Sprintf("%d", defaultPartitions)
	}
}

// RunAsKafkaReplacement starts a Streamline container configured as a drop-in Kafka replacement
func RunAsKafkaReplacement(ctx context.Context) (*StreamlineContainer, error) {
	return RunContainer(ctx, WithInMemory(), WithAutoCreateTopics(1))
}

// WithEphemeral enables ephemeral mode: in-memory, auto-cleanup, fastest startup.
func WithEphemeral() ContainerOption {
	return func(req *testcontainers.ContainerRequest) {
		req.Env["STREAMLINE_EPHEMERAL"] = "true"
		req.Env["STREAMLINE_IN_MEMORY"] = "true"
	}
}

// WithEphemeralIdleTimeout sets seconds to wait with zero connections before auto-shutdown.
func WithEphemeralIdleTimeout(seconds int) ContainerOption {
	return func(req *testcontainers.ContainerRequest) {
		req.Env["STREAMLINE_EPHEMERAL_IDLE_TIMEOUT"] = fmt.Sprintf("%d", seconds)
	}
}

// WithEphemeralAutoTopics pre-creates topics on startup (format: "name:partitions,name2:partitions2").
func WithEphemeralAutoTopics(topicSpecs string) ContainerOption {
	return func(req *testcontainers.ContainerRequest) {
		req.Env["STREAMLINE_EPHEMERAL_AUTO_TOPICS"] = topicSpecs
	}
}

// RunForTesting starts a container optimized for CI/CD: ephemeral, in-memory, auto-create, minimal logging.
func RunForTesting(ctx context.Context, opts ...ContainerOption) (*StreamlineContainer, error) {
	defaults := []ContainerOption{
		WithEphemeral(),
		WithAutoCreateTopics(3),
		WithLogLevel("warn"),
	}
	return RunContainer(ctx, append(defaults, opts...)...)
}

// helpers

func splitLines(s string) []string {
	return strings.Split(s, "\n")
}

func trimJSON(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "[]\",' \t")
	return s
}

func httpGet(_ context.Context, url string) (string, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return string(body), nil
}
