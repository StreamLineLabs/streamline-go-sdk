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

## License

Apache-2.0

## Security

To report a security vulnerability, please email **security@streamline.dev**.
Do **not** open a public issue.

See the [Security Policy](https://github.com/streamlinelabs/streamline/blob/main/SECURITY.md) for details.
