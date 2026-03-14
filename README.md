# Streamline Go SDK

[![CI](https://github.com/streamlinelabs/streamline-go-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/streamlinelabs/streamline-go-sdk/actions/workflows/ci.yml)
[![codecov](https://img.shields.io/codecov/c/github/streamlinelabs/streamline-go-sdk?style=flat-square)](https://codecov.io/gh/streamlinelabs/streamline-go-sdk)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.22%2B-cyan.svg)](https://go.dev/)
[![Docs](https://img.shields.io/badge/docs-streamlinelabs.dev-blue.svg)](https://streamlinelabs.dev/docs/sdks/go)

Official Go client for [Streamline](https://github.com/streamlinelabs/streamline-go-sdk) - The Redis of Streaming.

## Features

- Idiomatic Go with context support
- Producer with batching and compression
- Consumer with group coordination
- Admin client for topic management
- Query client for SQL analytics
- SASL/SCRAM authentication support
- TLS support

## Installation

```bash
go get github.com/streamlinelabs/streamline-go-sdk/streamline
```

## OpenTelemetry Tracing

The SDK includes built-in OpenTelemetry tracing via `TracingProducer` and
`TracingConsumer` wrappers. These wrappers automatically create spans for
produce and consume operations and propagate trace context through message
headers.

### Setup

```go
import "github.com/streamlinelabs/streamline-go-sdk/streamline"

// Wrap an existing producer with tracing
tracingProducer := streamline.NewTracingProducer(client.Producer)

// Wrap an existing consumer with tracing
tracingConsumer := streamline.NewTracingConsumer(consumer)
```

### Producing with Tracing

```go
// Sends a message with automatic span creation and context injection
result, err := tracingProducer.Send(ctx, "orders", []byte("key"), []byte("value"))
```

### Consuming with Tracing

```go
messages, errors := tracingConsumer.Start(ctx)
for msg := range messages {
    // Create a processing span linked to the producer trace
    processCtx, span := tracingConsumer.TraceProcess(ctx, msg)
    processMessage(processCtx, msg)
    span.End()
}
```

### Span Conventions

| Attribute | Value |
|-----------|-------|
| Span name | `{topic} {operation}` (e.g., "orders produce") |
| `messaging.system` | `streamline` |
| `messaging.destination.name` | Topic name |
| `messaging.operation` | `produce`, `consume`, or `process` |
| Span kind | `PRODUCER` for produce, `CONSUMER` for consume |

Trace context is propagated via W3C TraceContext headers in messages.

## Quick Start

```go
package main

import (
    "context"
    "log"

    "github.com/streamlinelabs/streamline-go-sdk/streamline"
)

func main() {
    // Create client
    config := streamline.DefaultConfig()
    config.Brokers = []string{"localhost:9092"}

    client, err := streamline.NewClient(config)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()

    // Produce a message
    result, err := client.Producer.Send(ctx, "my-topic", nil, []byte("Hello, World!"))
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Produced to partition %d at offset %d", result.Partition, result.Offset)
}
```

## Producer

### Simple Send

```go
result, err := client.Producer.Send(ctx, "topic", []byte("key"), []byte("value"))
```

### Send with Headers

```go
result, err := client.Producer.SendMessage(ctx, &streamline.Message{
    Topic: "topic",
    Key:   []byte("key"),
    Value: []byte("value"),
    Headers: map[string][]byte{
        "trace-id": []byte("abc123"),
    },
})
```

### Batch Send

```go
messages := []*streamline.Message{
    {Topic: "topic", Value: []byte("msg1")},
    {Topic: "topic", Value: []byte("msg2")},
    {Topic: "topic", Value: []byte("msg3")},
}
results, err := client.Producer.SendBatch(ctx, messages)
```

### Async Send

```go
resultCh := client.Producer.SendAsync(&streamline.Message{
    Topic: "topic",
    Value: []byte("async message"),
})

result := <-resultCh
if result.Err != nil {
    log.Printf("Error: %v", result.Err)
} else {
    log.Printf("Sent to partition %d", result.Partition)
}
```

## Consumer

### Consume Messages

```go
consumer, err := client.NewConsumer(ctx, "my-group", []string{"my-topic"})
if err != nil {
    log.Fatal(err)
}
defer consumer.Close()

messages, errors := consumer.Start(ctx)

for {
    select {
    case msg := <-messages:
        log.Printf("Received: %s", string(msg.Value))
    case err := <-errors:
        log.Printf("Error: %v", err)
    case <-ctx.Done():
        return
    }
}
```

### Poll Messages

```go
messages, err := consumer.Poll(ctx, 100, 5*time.Second)
for _, msg := range messages {
    log.Printf("Received: %s", string(msg.Value))
}
```

## Admin Client

### Create Topic

```go
err := client.Admin.CreateTopic(ctx, streamline.TopicConfig{
    Name:              "my-topic",
    NumPartitions:     3,
    ReplicationFactor: 1,
    Config: map[string]string{
        "retention.ms": "86400000",
    },
})
```

### List Topics

```go
topics, err := client.Admin.ListTopics(ctx)
for _, t := range topics {
    log.Printf("Topic: %s, Partitions: %d", t.Name, t.Partitions)
}
```

### Describe Topic

```go
info, partitions, err := client.Admin.DescribeTopic(ctx, "my-topic")
log.Printf("Topic: %s", info.Name)
for _, p := range partitions {
    log.Printf("  Partition %d: leader=%d, replicas=%v", p.ID, p.Leader, p.Replicas)
}
```

### Consumer Group Management

```go
// List consumer groups
groups, err := client.Admin.ListConsumerGroups(ctx)

// Describe consumer group
info, err := client.Admin.DescribeConsumerGroup(ctx, "my-group")

// Reset offsets to earliest
err := client.Admin.ResetConsumerGroupOffsets(ctx, "my-group", "my-topic", -2)

// Reset offsets to latest
err := client.Admin.ResetConsumerGroupOffsets(ctx, "my-group", "my-topic", -1)
```

### HTTP Admin (Cluster, Lag, Inspect, Metrics)

The `HTTPAdmin` client communicates with the Streamline HTTP REST API for operations not available via the Kafka wire protocol.

```go
admin := streamline.NewHTTPAdmin("http://localhost:9094")

// Cluster overview
cluster, err := admin.ClusterInfo(ctx)
fmt.Printf("Cluster: %s, Brokers: %d\n", cluster.ClusterID, len(cluster.Brokers))

// Consumer group lag monitoring
lag, err := admin.ConsumerGroupLag(ctx, "my-group")
fmt.Printf("Total lag: %d\n", lag.TotalLag)
for _, p := range lag.Partitions {
    fmt.Printf("  %s:%d lag=%d\n", p.Topic, p.Partition, p.Lag)
}

// Message inspection
messages, err := admin.InspectMessages(ctx, "events", 0, nil, 10)
for _, m := range messages {
    fmt.Printf("offset=%d key=%v value=%s\n", m.Offset, m.Key, m.Value)
}

// Latest messages
latest, err := admin.LatestMessages(ctx, "events", 5)

// Server metrics
metrics, err := admin.MetricsHistory(ctx)
for _, m := range metrics {
    fmt.Printf("%s=%f %v\n", m.Name, m.Value, m.Labels)
}
```

## Query Client

### Execute a Query

```go
queryClient := streamline.NewQueryClient("http://localhost:9094")

result, err := queryClient.Query(ctx, "SELECT * FROM topic('events') LIMIT 10")
if err != nil {
    log.Fatal(err)
}

for _, row := range result.Rows {
    fmt.Println(row)
}
fmt.Printf("Scanned %d rows in %dms\n", result.Metadata.RowsScanned, result.Metadata.ExecutionTimeMs)
```

### Query with Options

```go
opts := streamline.QueryOptions{
    TimeoutMs: 5000,
    MaxRows:   100,
}
result, err := queryClient.QueryWithOptions(ctx, "SELECT * FROM topic('events') ORDER BY offset DESC", opts)
```

### Explain Query Plan

```go
plan, err := queryClient.Explain(ctx, "SELECT * FROM topic('events') WHERE key = 'user-123'")
if err != nil {
    log.Fatal(err)
}
fmt.Println(plan)
```

## Configuration

### Producer Configuration

```go
config := streamline.DefaultConfig()
config.Producer = streamline.ProducerConfig{
    RequiredAcks:    -1,                      // All replicas
    Compression:     1,                       // gzip
    BatchSize:       16384,                   // 16KB
    BatchTimeout:    10 * time.Millisecond,
    Idempotent:      true,                    // Enable EOS
    Retries:         3,
}
```

### Consumer Configuration

```go
config.Consumer = streamline.ConsumerConfig{
    GroupID:           "my-group",
    AutoOffsetReset:   "earliest",
    SessionTimeout:    30 * time.Second,
    HeartbeatInterval: 3 * time.Second,
    MaxPollRecords:    500,
    IsolationLevel:    1,  // Read committed
}
```

### SASL Authentication

```go
config.SASL = &streamline.SASLConfig{
    Mechanism: "SCRAM-SHA-256",
    Username:  "user",
    Password:  "password",
}
```

### TLS Configuration

```go
config.TLS = &streamline.TLSConfig{
    Enable:   true,
    CertFile: "/path/to/client.crt",
    KeyFile:  "/path/to/client.key",
    CAFile:   "/path/to/ca.crt",
}
```

## Error Handling

All errors returned by the SDK implement the standard `error` interface. Structured errors are returned as `*StreamlineError` with error codes, hints, and retryability information.

```go
result, err := client.Producer.Send(ctx, "my-topic", nil, []byte("data"))
if err != nil {
    var se *streamline.StreamlineError
    if errors.As(err, &se) {
        switch se.Code {
        case streamline.ErrTopicNotFound:
            log.Printf("Topic missing: %s (hint: %s)", se.Message, se.Hint)
        case streamline.ErrAuthentication:
            log.Fatalf("Auth failed: %s", se.Message)
        case streamline.ErrTimeout:
            log.Printf("Timeout — retryable: %v", se.Retryable)
        default:
            log.Printf("Error [%s]: %s", se.Code, se.Message)
        }
    } else {
        log.Printf("Unexpected error: %v", err)
    }
}
```

### Error Codes

| Code | Constant | Retryable | Description |
|------|----------|-----------|-------------|
| `CONNECTION_ERROR` | `ErrConnection` | ✅ | Server unreachable or connection dropped |
| `AUTHENTICATION_ERROR` | `ErrAuthentication` | ❌ | SASL/TLS credentials rejected |
| `AUTHORIZATION_ERROR` | `ErrAuthorization` | ❌ | ACL denied the operation |
| `TOPIC_NOT_FOUND` | `ErrTopicNotFound` | ❌ | Topic does not exist |
| `TIMEOUT` | `ErrTimeout` | ✅ | Operation exceeded deadline |
| `PRODUCER_ERROR` | `ErrProducer` | ✅ | Send failed (network, batch, etc.) |
| `CONSUMER_ERROR` | `ErrConsumer` | ✅ | Consume/poll failed |
| `SERIALIZATION_ERROR` | `ErrSerialization` | ❌ | Message encode/decode failed |
| `CONFIGURATION_ERROR` | `ErrConfiguration` | ❌ | Invalid client configuration |
| `INTERNAL_ERROR` | `ErrInternal` | ❌ | Unexpected internal error |

### Helper Functions

```go
// Check if any error is a StreamlineError
if streamline.IsStreamlineError(err) {
    code := streamline.GetErrorCode(err)
    log.Printf("Streamline error code: %s", code)
}

// Check if an error is safe to retry
if streamline.IsRetryable(err) {
    // Back off and retry the operation
}

// Unwrap to inspect the underlying cause
var se *streamline.StreamlineError
if errors.As(err, &se) && se.Err != nil {
    log.Printf("Caused by: %v", se.Err)
}
```

## Circuit Breaker

The SDK includes a circuit breaker that protects your application from cascading failures. The `Producer` uses it automatically; you can also use it directly:

```go
import "github.com/streamlinelabs/streamline-go-sdk/streamline"

cb := streamline.NewCircuitBreaker(streamline.CircuitBreakerConfig{
    FailureThreshold: 5,                // Open after 5 consecutive failures
    SuccessThreshold: 2,                // Close after 2 half-open successes
    OpenTimeout:      30 * time.Second, // Probe interval
    OnStateChange: func(from, to streamline.CircuitState) {
        log.Printf("Circuit: %s → %s", from, to)
    },
})

if cb.Allow() {
    err := doSomething()
    if err != nil {
        cb.RecordFailure()
    } else {
        cb.RecordSuccess()
    }
}
```

When the circuit is open, `Allow()` returns `false` and operations are rejected immediately. See the [Circuit Breaker guide](https://streamlinelabs.dev/docs/features/circuit-breaker) for details.

## API Reference

### Client

| Method | Description |
|--------|-------------|
| `NewClient(config)` | Create a new client |
| `client.Close()` | Close the client |
| `client.NewConsumer(ctx, groupID, topics)` | Create a consumer |

### Producer

| Method | Description |
|--------|-------------|
| `Send(ctx, topic, key, value)` | Send a message synchronously |
| `SendMessage(ctx, msg)` | Send a message with full options |
| `SendAsync(msg)` | Send a message asynchronously |
| `SendBatch(ctx, messages)` | Send multiple messages |
| `Close()` | Close the producer |

### Consumer

| Method | Description |
|--------|-------------|
| `Start(ctx)` | Start consuming, returns message and error channels |
| `Poll(ctx, maxRecords, timeout)` | Poll for messages |
| `Commit()` | Commit offsets |
| `Close()` | Close the consumer |

### Admin

| Method | Description |
|--------|-------------|
| `CreateTopic(ctx, config)` | Create a topic |
| `DeleteTopic(ctx, name)` | Delete a topic |
| `ListTopics(ctx)` | List all topics |
| `DescribeTopic(ctx, name)` | Get topic details |
| `AddPartitions(ctx, name, count)` | Add partitions |
| `ListConsumerGroups(ctx)` | List consumer groups |
| `DescribeConsumerGroup(ctx, groupID)` | Get group details |
| `DeleteConsumerGroup(ctx, groupID)` | Delete a consumer group |
| `ResetConsumerGroupOffsets(ctx, groupID, topic, offset)` | Reset offsets |

### Query

| Method | Description |
|--------|-------------|
| `NewQueryClient(baseURL)` | Create a query client for the HTTP API |
| `Query(ctx, sql)` | Execute a SQL query with default options |
| `QueryWithOptions(ctx, sql, opts)` | Execute a SQL query with custom options |
| `Explain(ctx, sql)` | Get the query execution plan |

## Requirements

- Go 1.22 or later
- Streamline server 0.2.0 or later

## Examples

The [`examples/`](examples/) directory contains runnable examples:

| Example | Description |
|---------|-------------|
| [Basic Usage](examples/main.go) | Produce, consume, and admin operations |
| [Query Usage](examples/query_usage/main.go) | SQL analytics with the embedded query engine |
| [Schema Registry](examples/schema_registry/main.go) | Schema registration and validation |
| [Circuit Breaker](examples/circuit_breaker/main.go) | Resilient production with circuit breaker |
| [Security](examples/security/main.go) | TLS and SASL authentication |

Run any example:

```bash
go run examples/main.go
go run examples/circuit_breaker/main.go
```

## Contributing

Contributions are welcome! Please see the [organization contributing guide](https://github.com/streamlinelabs/.github/blob/main/CONTRIBUTING.md) for guidelines.

## License

Apache-2.0

## Security

To report a security vulnerability, please email **security@streamline.dev**.
Do **not** open a public issue.

See the [Security Policy](https://github.com/streamlinelabs/streamline/blob/main/SECURITY.md) for details.

<!-- add godoc examples for consumer API -->

