# Testcontainers Streamline (Go)

Testcontainers module for [Streamline](https://github.com/streamlinelabs/streamline) - The Redis of Streaming.

## Features

- Kafka-compatible container for testing
- Fast startup (~100ms vs seconds for Kafka)
- Low memory footprint (<50MB)
- No ZooKeeper or KRaft required
- Built-in health checks

## Installation

```bash
go get github.com/streamlinelabs/streamline-go-sdk/testcontainers
```

## Usage

### Basic Usage

```go
package main

import (
    "context"
    "log"

    streamline "github.com/streamlinelabs/streamline-go-sdk/testcontainers"
    "github.com/twmb/franz-go/pkg/kgo"
)

func main() {
    ctx := context.Background()

    // Start container
    container, err := streamline.RunContainer(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer container.Terminate(ctx)

    // Get bootstrap servers
    bootstrapServers, err := container.BootstrapServers(ctx)
    if err != nil {
        log.Fatal(err)
    }

    // Use with franz-go or any Kafka client
    client, err := kgo.NewClient(
        kgo.SeedBrokers(bootstrapServers),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Produce a message
    record := &kgo.Record{Topic: "test-topic", Value: []byte("hello")}
    if err := client.ProduceSync(ctx, record).FirstErr(); err != nil {
        log.Fatal(err)
    }
}
```

### With Options

```go
container, err := streamline.RunContainer(ctx,
    streamline.WithDebugLogging(),
    streamline.WithTag("0.2.0"),
)
```

### Create Topics

```go
err := container.CreateTopic(ctx, "my-topic", 3) // 3 partitions
```

### Access HTTP API

```go
healthURL, _ := container.HealthEndpoint(ctx)
metricsURL, _ := container.MetricsEndpoint(ctx)
```

## API Reference

### Functions

| Function | Description |
|----------|-------------|
| `RunContainer(ctx, ...opts)` | Starts a new Streamline container |
| `WithImage(image)` | Sets a custom Docker image |
| `WithTag(tag)` | Sets the Docker image tag |
| `WithLogLevel(level)` | Sets log level |
| `WithDebugLogging()` | Enables debug logging |
| `WithTraceLogging()` | Enables trace logging |

### StreamlineContainer Methods

| Method | Description |
|--------|-------------|
| `BootstrapServers(ctx)` | Returns Kafka bootstrap servers |
| `HTTPEndpoint(ctx)` | Returns HTTP API base URL |
| `HealthEndpoint(ctx)` | Returns health check URL |
| `MetricsEndpoint(ctx)` | Returns metrics URL |
| `CreateTopic(ctx, name, partitions)` | Creates a topic |

## Testing

```go
func TestWithStreamline(t *testing.T) {
    ctx := context.Background()

    container, err := streamline.RunContainer(ctx)
    require.NoError(t, err)
    defer container.Terminate(ctx)

    bootstrapServers, err := container.BootstrapServers(ctx)
    require.NoError(t, err)

    // Your test code here
}
```

## License

Apache-2.0
