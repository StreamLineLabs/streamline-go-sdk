# CLAUDE.md — Streamline Go SDK

## Overview
Go SDK for [Streamline](https://github.com/streamlinelabs/streamline), built on IBM Sarama. Communicates via the Kafka wire protocol on port 9092.

## Build & Test
```bash
go build ./...            # Build (unit/default build — no CGO, no server)
go test ./...             # Run tests (self-contained: no broker, server, or Docker)
go vet ./...              # Vet
go fmt ./...              # Format
golangci-lint run         # Lint (golangci-lint v2 config)
```

Build tags gate everything that needs an external dependency:

```bash
go test -tags=integration ./...              # Conformance suite; needs a live server
go vet  -tags=integration ./...              # Compile-check the suite without a server
CGO_ENABLED=1 go build -tags embedded ./...  # embedded/ CGO bindings; needs libstreamline
make integration-test                        # docker compose + tagged conformance run
```

`STREAMLINE_BOOTSTRAP`, `STREAMLINE_HTTP`, `STREAMLINE_AUTH_ENABLED`, and
`STREAMLINE_SKIP_INTEGRATION` control the tagged conformance suite.

## Architecture
```
streamline/
├── client.go             # Client, Config (+ withDefaults/buildSaramaConfig), SASL/TLS config types
├── producer.go           # Producer with batching & compression
├── consumer.go           # Consumer with group coordination
├── admin.go              # Topic/group management
├── errors.go             # StreamlineError with ErrorCode, IsRetryable()
├── telemetry.go          # OpenTelemetry TracingProducer/TracingConsumer wrappers
├── *_test.go             # Tests inline with source
embedded/                 # CGO bindings (tag: embedded) + no-op stub for default builds
conformance_test.go       # Live-server conformance suite (tag: integration)
testutil_test.go          # Live-server test selection helpers (env + skip reasons)
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
- Unit tests: `*_test.go` files inline with source; they must stay self-contained
  (no broker, HTTP server, or Docker) so `go test ./...` works offline
- Benchmarks: `benchmark_test.go`
- Conformance/live-server tests: root `conformance_test.go`, behind the
  `integration` build tag; call `SkipIfNoServer(t)`/`RequireIntegration(t)` from
  `testutil_test.go` instead of ad-hoc `testing.Short()` checks
- Testcontainers module: `testcontainers/` (separate `go.mod`, needs Docker)
