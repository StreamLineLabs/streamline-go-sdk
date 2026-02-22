# CLAUDE.md — Streamline Go SDK

## Overview
Go SDK for [Streamline](https://github.com/streamlinelabs/streamline), built on IBM Sarama. Communicates via the Kafka wire protocol on port 9092.

## Build & Test
```bash
go build ./...            # Build
go test ./...             # Run tests
go vet ./...              # Vet
go fmt ./...              # Format
golangci-lint run         # Lint (14 linters configured)
```

## Architecture
```
streamline/
├── client.go             # Client — main entry point with Config
├── producer.go           # Producer with batching & compression
├── consumer.go           # Consumer with group coordination
├── admin.go              # Topic/group management
├── config.go             # Config struct with nested SASLConfig, TLSConfig
├── errors.go             # StreamlineError with ErrorCode, IsRetryable()
├── telemetry.go          # OpenTelemetry TracingProducer/TracingConsumer wrappers
├── *_test.go             # Tests inline with source
```

## Coding Conventions
- **Context-first**: All public functions take `context.Context` as first parameter
- **Error wrapping**: Use `fmt.Errorf("...: %w", err)` for error chains
- **Custom errors**: Use `StreamlineError` with `NewConnectionError()`, `NewTimeoutError()` factories
- **Config pattern**: Struct composition for options (not functional options)
- **Naming**: Go standard — exported PascalCase, unexported camelCase
- **No `panic`**: Return errors instead; panics are only for truly unrecoverable programmer bugs

## Error Handling Pattern
```go
msg, err := consumer.Consume(ctx, "topic")
if err != nil {
    var sErr *streamline.StreamlineError
    if errors.As(err, &sErr) && sErr.IsRetryable() {
        // Retry
    }
    return err
}
```

## Dependencies
- `github.com/IBM/sarama` v1.43.0 — Kafka protocol
- `go.opentelemetry.io/otel` v1.24.0 — Tracing
- SCRAM auth support for SASL

## Testing
- Unit tests: `*_test.go` files inline with source
- Benchmarks: `benchmark_test.go`
- Integration tests: `testcontainers/` with Docker Compose
