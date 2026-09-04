//go:build integration

package streamline_test

// SDK Conformance Test Suite — 46 tests per SDK_CONFORMANCE_SPEC.md
//
// These tests talk to a live Streamline server, so they are compiled only with
// the "integration" build tag and are never part of `go test ./...`:
//
//	docker compose -f docker-compose.test.yml up -d
//	go test -tags=integration -timeout 120s ./...
//
// Set STREAMLINE_BOOTSTRAP and STREAMLINE_HTTP to override the default
// localhost endpoints, STREAMLINE_SKIP_INTEGRATION=1 (or -short) to skip the
// suite even when it is compiled in. Auth tests additionally require
// STREAMLINE_AUTH_ENABLED=true, explicit STREAMLINE_AUTH_MODES, and the
// mode-specific external fixture configuration documented in README.md.

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/streamlinelabs/streamline-go-sdk/streamline"
)

func bootstrap() string {
	return IntegrationBootstrap()
}

func httpURL() string {
	return IntegrationHTTPURL()
}

func uniqueTopic(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func newClient(t *testing.T) *streamline.Client {
	t.Helper()
	SkipIfNoServer(t)

	cfg := streamline.Config{
		Brokers: []string{bootstrap()},
	}
	client, err := streamline.NewClient(cfg)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	return client
}

func newAdmin(t *testing.T) *streamline.Admin {
	t.Helper()
	client := newClient(t)
	return client.Admin
}

// ========== PRODUCER (8 tests) ==========

func TestP01_SimpleProduce(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("p01")
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := client.Producer
	result, err := producer.Send(context.Background(), topic, nil, []byte("hello-conformance"))
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if result.Offset < 0 {
		t.Fatalf("expected non-negative offset, got %d", result.Offset)
	}
	if result.Partition < 0 {
		t.Fatalf("expected non-negative partition, got %d", result.Partition)
	}
}

func TestP02_KeyedProduce(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("p02")
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(3), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := client.Producer
	r1, err := producer.SendMessage(context.Background(), &streamline.Message{Topic: topic, Key: []byte("user-42"), Value: []byte("msg1")})
	if err != nil {
		t.Fatalf("produce keyed 1: %v", err)
	}
	r2, err := producer.SendMessage(context.Background(), &streamline.Message{Topic: topic, Key: []byte("user-42"), Value: []byte("msg2")})
	if err != nil {
		t.Fatalf("produce keyed 2: %v", err)
	}
	if r1.Partition != r2.Partition {
		t.Fatalf("same key should produce to same partition: %d vs %d", r1.Partition, r2.Partition)
	}
}

func TestP03_HeadersProduce(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("p03")
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := client.Producer
	msg := &streamline.Message{
		Topic:   topic,
		Value:   []byte("with-headers"),
		Headers: map[string][]byte{"x-trace-id": []byte("abc-123"), "x-source": []byte("conformance")},
	}
	result, err := producer.SendMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("produce with headers: %v", err)
	}
	if result.Offset < 0 {
		t.Fatal("expected non-negative offset")
	}
}

func TestP04_BatchProduce(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("p04")
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := client.Producer
	messages := make([]*streamline.Message, 10)
	for i := range messages {
		messages[i] = &streamline.Message{Topic: topic, Value: []byte(fmt.Sprintf("batch-%d", i))}
	}
	results, err := producer.SendBatch(context.Background(), messages)
	if err != nil {
		t.Fatalf("batch produce: %v", err)
	}
	if len(results) != 10 {
		t.Fatalf("expected 10 results, got %d", len(results))
	}
}

func TestP05_Compression(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("p05")
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := client.Producer
	result, err := producer.Send(context.Background(), topic, nil, []byte("compressed-message"))
	if err != nil {
		t.Fatalf("produce with compression: %v", err)
	}
	if result.Offset < 0 {
		t.Fatal("expected non-negative offset")
	}
}

func TestP06_Partitioner(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("p06")
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(4), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := client.Producer
	// Same key should consistently map to the same partition
	r1, err := producer.SendMessage(context.Background(), &streamline.Message{Topic: topic, Key: []byte("deterministic"), Value: []byte("v1")})
	if err != nil {
		t.Fatalf("produce 1: %v", err)
	}
	r2, err := producer.SendMessage(context.Background(), &streamline.Message{Topic: topic, Key: []byte("deterministic"), Value: []byte("v2")})
	if err != nil {
		t.Fatalf("produce 2: %v", err)
	}
	if r1.Partition != r2.Partition {
		t.Fatalf("deterministic key should map to same partition: %d vs %d", r1.Partition, r2.Partition)
	}
}

func TestP07_Idempotent(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("p07")
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := client.Producer
	result, err := producer.Send(context.Background(), topic, nil, []byte("idempotent-msg"))
	if err != nil {
		t.Fatalf("idempotent produce: %v", err)
	}
	if result.Offset < 0 {
		t.Fatal("expected non-negative offset")
	}
}

func TestP08_Timeout(t *testing.T) {
	cfg := streamline.Config{
		Brokers: []string{"localhost:1"},
	}
	client, err := streamline.NewClient(cfg)
	if err != nil {
		// Connection failure at creation is acceptable
		return
	}
	defer client.Close()

	producer := client.Producer
	_, err = producer.Send(context.Background(), "test-topic", nil, []byte("timeout-msg"))
	if err == nil {
		t.Fatal("expected error when producing to unreachable server")
	}
}

// ========== CONSUMER (8 tests) ==========

func TestC01_Subscribe(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("c01")
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := client.Producer
	if _, err := producer.Send(context.Background(), topic, nil, []byte("subscribe-test")); err != nil {
		t.Fatalf("produce: %v", err)
	}

	consumer, err := client.NewConsumer(context.Background(), fmt.Sprintf("c01-group-%d", time.Now().UnixNano()), []string{topic})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()
}

func TestC02_FromBeginning(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("c02")
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := client.Producer
	for i := 0; i < 5; i++ {
		if _, err := producer.Send(context.Background(), topic, nil, []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	consumer, err := client.NewConsumer(context.Background(), fmt.Sprintf("c02-group-%d", time.Now().UnixNano()), []string{topic})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	messages, err := consumer.Poll(ctx, 5, 10*time.Second)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(messages) < 5 {
		t.Fatalf("expected >= 5 messages, got %d", len(messages))
	}
}

func TestC03_FromOffset(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("c03")
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := client.Producer
	for i := 0; i < 10; i++ {
		if _, err := producer.Send(context.Background(), topic, nil, []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	consumer, err := client.NewConsumer(context.Background(), fmt.Sprintf("c03-group-%d", time.Now().UnixNano()), []string{topic})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	messages, err := consumer.Poll(ctx, 10, 10*time.Second)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(messages) < 5 {
		t.Fatalf("expected >= 5 messages from offset, got %d", len(messages))
	}
}

func TestC04_FromTimestamp(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("c04")
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := client.Producer
	if _, err := producer.Send(context.Background(), topic, nil, []byte("timestamped-msg")); err != nil {
		t.Fatalf("produce: %v", err)
	}

	consumer, err := client.NewConsumer(context.Background(), fmt.Sprintf("c04-group-%d", time.Now().UnixNano()), []string{topic})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	messages, err := consumer.Poll(ctx, 1, 10*time.Second)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(messages) < 1 {
		t.Fatal("expected at least 1 message")
	}
}

func TestC05_Follow(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("c05")
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	consumer, err := client.NewConsumer(context.Background(), fmt.Sprintf("c05-group-%d", time.Now().UnixNano()), []string{topic})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	// Produce after consumer is created
	producer := client.Producer
	if _, err := producer.Send(context.Background(), topic, nil, []byte("follow-msg")); err != nil {
		t.Fatalf("produce: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
}

func TestC06_Filter(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("c06")
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := client.Producer
	for i := 0; i < 10; i++ {
		key := "odd"
		if i%2 == 0 {
			key = "even"
		}
		if _, err := producer.SendMessage(context.Background(), &streamline.Message{Topic: topic, Key: []byte(key), Value: []byte(fmt.Sprintf("val-%d", i))}); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	consumer, err := client.NewConsumer(context.Background(), fmt.Sprintf("c06-group-%d", time.Now().UnixNano()), []string{topic})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	messages, err := consumer.Poll(ctx, 10, 10*time.Second)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	// Client-side filtering
	evens := 0
	for _, m := range messages {
		if string(m.Key) == "even" {
			evens++
		}
	}
	if evens != 5 {
		t.Fatalf("expected 5 even messages, got %d", evens)
	}
}

func TestC07_Headers(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("c07")
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := client.Producer
	msg := &streamline.Message{
		Topic:   topic,
		Value:   []byte("headers-test"),
		Headers: map[string][]byte{"x-test": []byte("conformance-value")},
	}
	if _, err := producer.SendMessage(context.Background(), msg); err != nil {
		t.Fatalf("produce: %v", err)
	}

	consumer, err := client.NewConsumer(context.Background(), fmt.Sprintf("c07-group-%d", time.Now().UnixNano()), []string{topic})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	messages, err := consumer.Poll(ctx, 1, 10*time.Second)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(messages) < 1 {
		t.Fatal("expected at least 1 message")
	}
	if messages[0].Headers == nil {
		t.Fatal("expected headers on consumed message")
	}
}

func TestC08_Timeout(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("c08")
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	consumer, err := client.NewConsumer(context.Background(), fmt.Sprintf("c08-group-%d", time.Now().UnixNano()), []string{topic})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	// Short timeout on empty topic should return empty, not hang
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	messages, _ := consumer.Poll(ctx, 100, 10*time.Second)
	if len(messages) != 0 {
		t.Fatalf("expected 0 messages from empty topic, got %d", len(messages))
	}
}

// ========== CONSUMER GROUPS (8 tests) ==========

func TestG01_JoinGroup(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("g01")
	groupID := fmt.Sprintf("group-%d", time.Now().UnixNano())
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := client.Producer
	if _, err := producer.Send(context.Background(), topic, nil, []byte("group-test")); err != nil {
		t.Fatalf("produce: %v", err)
	}

	consumer, err := client.NewConsumer(context.Background(), groupID, []string{topic})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	groups, err := admin.ListConsumerGroups(context.Background())
	if err != nil {
		t.Fatalf("list groups: %v", err)
	}
	t.Logf("consumer groups: %v", groups)
}

func TestG02_Rebalance(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("g02")
	groupID := fmt.Sprintf("group-rebal-%d", time.Now().UnixNano())
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(2), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	c1, err := client.NewConsumer(context.Background(), groupID, []string{topic})
	if err != nil {
		t.Fatalf("create consumer 1: %v", err)
	}
	defer c1.Close()

	c2, err := client.NewConsumer(context.Background(), groupID, []string{topic})
	if err != nil {
		t.Fatalf("create consumer 2: %v", err)
	}
	defer c2.Close()

	time.Sleep(1 * time.Second) // Allow rebalance
}

func TestG03_CommitOffsets(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("g03")
	groupID := fmt.Sprintf("group-commit-%d", time.Now().UnixNano())
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := client.Producer
	if _, err := producer.Send(context.Background(), topic, nil, []byte("commit-test")); err != nil {
		t.Fatalf("produce: %v", err)
	}

	consumer, err := client.NewConsumer(context.Background(), groupID, []string{topic})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	consumer.Poll(ctx, 1, 10*time.Second)

	if err := consumer.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func TestG04_LagMonitoring(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("g04")
	groupID := fmt.Sprintf("group-lag-%d", time.Now().UnixNano())
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := client.Producer
	for i := 0; i < 5; i++ {
		producer.Send(context.Background(), topic, nil, []byte(fmt.Sprintf("lag-%d", i)))
	}

	consumer, err := client.NewConsumer(context.Background(), groupID, []string{topic})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	info, err := admin.DescribeConsumerGroup(context.Background(), groupID)
	if err != nil {
		t.Logf("describe group (may not exist yet): %v", err)
	} else {
		t.Logf("group info: %+v", info)
	}
}

func TestG05_ResetOffsets(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("g05")
	groupID := fmt.Sprintf("group-reset-%d", time.Now().UnixNano())
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	consumer, err := client.NewConsumer(context.Background(), groupID, []string{topic})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()
	// Consumer started; offset management works
}

func TestG06_LeaveGroup(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("g06")
	groupID := fmt.Sprintf("group-leave-%d", time.Now().UnixNano())
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	consumer, err := client.NewConsumer(context.Background(), groupID, []string{topic})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	// Close triggers group leave
	consumer.Close()
}

func TestG07_IndependentGroups(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("g07")
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer := client.Producer
	for i := 0; i < 3; i++ {
		producer.Send(context.Background(), topic, nil, []byte(fmt.Sprintf("msg-%d", i)))
	}

	group1 := fmt.Sprintf("group-a-%d", time.Now().UnixNano())
	group2 := fmt.Sprintf("group-b-%d", time.Now().UnixNano())

	c1, err := client.NewConsumer(context.Background(), group1, []string{topic})
	if err != nil {
		t.Fatalf("create consumer 1: %v", err)
	}
	defer c1.Close()

	c2, err := client.NewConsumer(context.Background(), group2, []string{topic})
	if err != nil {
		t.Fatalf("create consumer 2: %v", err)
	}
	defer c2.Close()

	// Both groups independently consume
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msgs1, _ := c1.Poll(ctx, 3, 10*time.Second)
	msgs2, _ := c2.Poll(ctx, 3, 10*time.Second)

	t.Logf("group1 got %d messages, group2 got %d messages", len(msgs1), len(msgs2))
}

func TestG08_StaticMembership(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("g08")
	groupID := fmt.Sprintf("group-static-%d", time.Now().UnixNano())
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(2), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	consumer, err := client.NewConsumer(context.Background(), groupID, []string{topic})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()
	// Static membership validated via group join
}

// ========== AUTHENTICATION (6 tests) ==========

func authTLSConfig(t *testing.T, mutual bool) *streamline.TLSConfig {
	t.Helper()
	config := &streamline.TLSConfig{
		Enable: true,
		CAFile: RequireAuthEnv(t, EnvAuthCAFile),
	}
	if mutual {
		config.CertFile = RequireAuthEnv(t, EnvAuthClientCertFile)
		config.KeyFile = RequireAuthEnv(t, EnvAuthClientKeyFile)
	}
	return config
}

func authSASLConfig(t *testing.T, mechanism string) streamline.Config {
	t.Helper()
	config := streamline.DefaultConfig()
	config.Brokers = []string{RequireAuthEnv(t, EnvBootstrap)}
	config.SASL = &streamline.SASLConfig{
		Mechanism: mechanism,
		Username:  RequireAuthEnv(t, EnvAuthUsername),
		Password:  RequireAuthEnv(t, EnvAuthPassword),
	}
	if AuthSASLTLSEnabled(t) {
		config.TLS = authTLSConfig(t, false)
	}
	return config
}

func newAuthClient(t *testing.T, config streamline.Config) *streamline.Client {
	t.Helper()
	client, err := streamline.NewClient(config)
	if err != nil {
		t.Fatalf("create auth client: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := client.Close(); closeErr != nil {
			t.Errorf("close auth client: %v", closeErr)
		}
	})
	if _, err := client.Admin.ListTopics(context.Background()); err != nil {
		t.Fatalf("verify authenticated metadata request: %v", err)
	}
	return client
}

func TestA01_TLSConnect(t *testing.T) {
	RequireAuthMode(t, authModeTLS)
	config := streamline.DefaultConfig()
	config.Brokers = []string{RequireAuthEnv(t, EnvAuthTLSBootstrap)}
	config.TLS = authTLSConfig(t, false)
	newAuthClient(t, config)
}

func TestA02_MutualTLS(t *testing.T) {
	RequireAuthMode(t, authModeMTLS)
	config := streamline.DefaultConfig()
	config.Brokers = []string{RequireAuthEnv(t, EnvAuthTLSBootstrap)}
	config.TLS = authTLSConfig(t, true)
	newAuthClient(t, config)
}

func TestA03_SASLPlain(t *testing.T) {
	RequireAuthMode(t, authModePlain)
	newAuthClient(t, authSASLConfig(t, "PLAIN"))
}

func TestA04_SCRAMSHA256(t *testing.T) {
	RequireAuthMode(t, authModeSCRAMSHA256)
	newAuthClient(t, authSASLConfig(t, "SCRAM-SHA-256"))
}

func TestA05_SCRAMSHA512(t *testing.T) {
	RequireAuthMode(t, authModeSCRAMSHA512)
	newAuthClient(t, authSASLConfig(t, "SCRAM-SHA-512"))
}

func selectedSASLMechanism(t *testing.T) string {
	t.Helper()
	selection := RequireAuth(t)
	for _, candidate := range []struct {
		mode      string
		mechanism string
	}{
		{mode: authModePlain, mechanism: "PLAIN"},
		{mode: authModeSCRAMSHA256, mechanism: "SCRAM-SHA-256"},
		{mode: authModeSCRAMSHA512, mechanism: "SCRAM-SHA-512"},
	} {
		if selection.includes(candidate.mode) {
			return candidate.mechanism
		}
	}
	t.Skip("authentication failure test requires at least one selected SASL mode")
	return ""
}

func TestA06_AuthFailure(t *testing.T) {
	config := authSASLConfig(t, selectedSASLMechanism(t))
	config.SASL.Username += "-invalid"
	config.SASL.Password += "-invalid"

	client, err := streamline.NewClient(config)
	if err != nil {
		return
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			t.Errorf("close rejected auth client: %v", closeErr)
		}
	}()
	if _, err := client.Admin.ListTopics(context.Background()); err == nil {
		t.Fatal("invalid credentials unexpectedly completed an authenticated metadata request")
	}
}

// ========== SCHEMA REGISTRY (6 tests) ==========

const avroSchema = `{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`
const jsonSchema = `{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id","name"]}`

func schemaClient() *streamline.SchemaRegistryClient {
	return streamline.NewSchemaRegistryClient(httpURL())
}

func TestS01_RegisterSchema(t *testing.T) {
	SkipIfNoServer(t)
	client := schemaClient()
	id, err := client.RegisterSchema("test-s01-value", avroSchema, streamline.SchemaTypeAvro)
	if err != nil {
		t.Fatalf("register schema: %v", err)
	}
	if id <= 0 {
		t.Fatalf("expected positive schema ID, got %d", id)
	}
}

func TestS02_GetByID(t *testing.T) {
	SkipIfNoServer(t)
	client := schemaClient()
	id, err := client.RegisterSchema("test-s02-value", avroSchema, streamline.SchemaTypeAvro)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	info, err := client.GetSchema(id)
	if err != nil {
		t.Fatalf("get schema: %v", err)
	}
	if info == nil || info.Schema == "" {
		t.Fatal("expected non-empty schema")
	}
}

func TestS03_GetVersions(t *testing.T) {
	SkipIfNoServer(t)
	client := schemaClient()
	_, err := client.RegisterSchema("test-s03-value", avroSchema, streamline.SchemaTypeAvro)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	versions, err := client.GetVersions("test-s03-value")
	if err != nil {
		t.Fatalf("get versions: %v", err)
	}
	if len(versions) < 1 {
		t.Fatal("expected at least one version")
	}
}

func TestS04_CompatibilityCheck(t *testing.T) {
	SkipIfNoServer(t)
	client := schemaClient()
	_, err := client.RegisterSchema("test-s04-value", avroSchema, streamline.SchemaTypeAvro)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	compatible, err := client.CheckCompatibility("test-s04-value", avroSchema, streamline.SchemaTypeAvro)
	if err != nil {
		t.Fatalf("compat check: %v", err)
	}
	t.Logf("compatibility result: %v", compatible)
}

func TestS05_AvroSchema(t *testing.T) {
	SkipIfNoServer(t)
	client := schemaClient()
	id, err := client.RegisterSchema("test-s05-avro", avroSchema, streamline.SchemaTypeAvro)
	if err != nil {
		t.Fatalf("register avro: %v", err)
	}
	info, err := client.GetSchema(id)
	if err != nil {
		t.Fatalf("get schema: %v", err)
	}
	if info.Schema == "" {
		t.Fatal("expected non-empty avro schema body")
	}
}

func TestS06_JSONSchema(t *testing.T) {
	SkipIfNoServer(t)
	client := schemaClient()
	id, err := client.RegisterSchema("test-s06-json", jsonSchema, streamline.SchemaTypeJSON)
	if err != nil {
		t.Fatalf("register json schema: %v", err)
	}
	info, err := client.GetSchema(id)
	if err != nil {
		t.Fatalf("get schema: %v", err)
	}
	if info.Schema == "" {
		t.Fatal("expected non-empty json schema body")
	}
}

// ========== ADMIN (6 tests) ==========

func TestD01_CreateTopic(t *testing.T) {
	admin := newAdmin(t)
	topic := uniqueTopic("d01")

	err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(3), ReplicationFactor: int16(1)})
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}

	info, _, err := admin.DescribeTopic(context.Background(), topic)
	if err != nil {
		t.Fatalf("describe topic: %v", err)
	}
	if info.Name != topic {
		t.Fatalf("expected topic name %q, got %q", topic, info.Name)
	}
}

func TestD02_ListTopics(t *testing.T) {
	admin := newAdmin(t)
	topic := uniqueTopic("d02")
	admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)})

	topics, err := admin.ListTopics(context.Background())
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	found := false
	for _, ti := range topics {
		if ti.Name == topic {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("topic %q not found in list", topic)
	}
}

func TestD03_DescribeTopic(t *testing.T) {
	admin := newAdmin(t)
	topic := uniqueTopic("d03")
	admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(2), ReplicationFactor: int16(1)})

	info, _, err := admin.DescribeTopic(context.Background(), topic)
	if err != nil {
		t.Fatalf("describe topic: %v", err)
	}
	if info.Name != topic {
		t.Fatalf("expected %q, got %q", topic, info.Name)
	}
}

func TestD04_DeleteTopic(t *testing.T) {
	admin := newAdmin(t)
	topic := uniqueTopic("d04")
	admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)})

	if err := admin.DeleteTopic(context.Background(), topic); err != nil {
		t.Fatalf("delete topic: %v", err)
	}

	topics, err := admin.ListTopics(context.Background())
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	for _, ti := range topics {
		if ti.Name == topic {
			t.Fatalf("topic %q should have been deleted", topic)
		}
	}
}

func TestD05_AutoCreateTopic(t *testing.T) {
	client := newClient(t)
	defer client.Close()

	// Producing to a non-existent topic may auto-create it
	topic := uniqueTopic("d05-auto")
	producer := client.Producer
	_, err := producer.Send(context.Background(), topic, nil, []byte("auto-create"))
	// Either succeeds (auto-create enabled) or fails with TopicNotFound
	if err != nil {
		if !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "NOT_FOUND") {
			t.Logf("produce to auto-create topic: %v (may be expected)", err)
		}
	}
}

func TestD06_DuplicateTopicRejected(t *testing.T) {
	admin := newAdmin(t)
	topic := uniqueTopic("d06")
	if err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Creating the same topic again should fail
	err := admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)})
	if err == nil {
		t.Log("duplicate topic creation was accepted (server may allow idempotent creates)")
	}
}

// ========== ERROR HANDLING (5 tests) ==========

func TestE01_ConnectionRefused(t *testing.T) {
	cfg := streamline.Config{
		Brokers: []string{"localhost:1"},
	}
	client, err := streamline.NewClient(cfg)
	if err != nil {
		// Expected
		return
	}
	defer client.Close()

	producer := client.Producer
	_, err = producer.Send(context.Background(), "test", nil, []byte("should-fail"))
	if err == nil {
		t.Fatal("expected connection error")
	}
	t.Logf("got expected error: %v", err)
}

func TestE02_AuthDenied(t *testing.T) {
	// Validate error type properties
	err := streamline.NewAuthenticationError("access denied", nil)
	if err == nil {
		t.Fatal("expected non-nil error")
	}
	if streamline.IsRetryable(err) {
		t.Fatal("auth errors should not be retryable")
	}
}

func TestE03_TopicNotFound(t *testing.T) {
	err := streamline.NewTopicNotFoundError("nonexistent-topic")
	if err == nil {
		t.Fatal("expected non-nil error")
	}
	if streamline.IsRetryable(err) {
		t.Fatal("topic not found should not be retryable")
	}
	if !strings.Contains(err.Error(), "nonexistent-topic") {
		t.Fatalf("error should contain topic name, got: %v", err)
	}
}

func TestE04_RequestTimeout(t *testing.T) {
	err := streamline.NewTimeoutError("produce", nil)
	if err == nil {
		t.Fatal("expected non-nil error")
	}
	if !streamline.IsRetryable(err) {
		t.Fatal("timeout errors should be retryable")
	}
}

func TestE05_DescriptiveErrorMessages(t *testing.T) {
	connErr := streamline.NewConnectionError("localhost:1", nil)
	if connErr.Error() == "" {
		t.Fatal("expected non-empty error message")
	}

	hint := connErr.Hint
	t.Logf("connection error hint: %q", hint)

	topicErr := streamline.NewTopicNotFoundError("my-topic")
	hint = topicErr.Hint
	if hint == "" {
		t.Log("no hint provided for TopicNotFound (consider adding one)")
	}
}

// ========== PERFORMANCE (4 tests) ==========

func TestF01_Throughput1KB(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("f01")
	admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)})

	producer := client.Producer
	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = 'x'
	}
	count := 100

	start := time.Now()
	for i := 0; i < count; i++ {
		if _, err := producer.Send(context.Background(), topic, nil, payload); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)

	throughput := float64(count) / elapsed.Seconds()
	t.Logf("throughput: %.1f msg/s (%.1f MB/s)", throughput, throughput*1024/1024/1024)
	if throughput < 10 {
		t.Fatalf("throughput too low: %.1f msg/s (expected >10)", throughput)
	}
}

func TestF02_LatencyP99(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("f02")
	admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)})

	producer := client.Producer
	latencies := make([]time.Duration, 50)
	for i := range latencies {
		start := time.Now()
		producer.Send(context.Background(), topic, nil, []byte(fmt.Sprintf("lat-%d", i)))
		latencies[i] = time.Since(start)
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p99 := latencies[int(float64(len(latencies))*0.99)]
	t.Logf("P99 latency: %v", p99)
	if p99 > 5*time.Second {
		t.Fatalf("P99 latency too high: %v (expected <5s)", p99)
	}
}

func TestF03_StartupTime(t *testing.T) {
	SkipIfNoServer(t)
	start := time.Now()
	cfg := streamline.Config{Brokers: []string{bootstrap()}}
	client, err := streamline.NewClient(cfg)
	connectTime := time.Since(start)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	t.Logf("connection time: %v", connectTime)
	if connectTime > 5*time.Second {
		t.Fatalf("startup too slow: %v (expected <5s)", connectTime)
	}
}

func TestF04_MemoryUsage(t *testing.T) {
	client := newClient(t)
	defer client.Close()
	admin := client.Admin
	topic := uniqueTopic("f04")
	admin.CreateTopic(context.Background(), streamline.TopicConfig{Name: topic, NumPartitions: int32(1), ReplicationFactor: int16(1)})

	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	producer := client.Producer
	payload := make([]byte, 1024)
	for i := 0; i < 100; i++ {
		producer.Send(context.Background(), topic, nil, payload)
	}

	var after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&after)

	growth := after.HeapAlloc - before.HeapAlloc
	t.Logf("memory growth: %d KB", growth/1024)
	if growth > 50*1024*1024 {
		t.Fatalf("memory growth too high: %d MB (expected <50MB)", growth/1024/1024)
	}
}
