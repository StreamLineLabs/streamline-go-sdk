# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Fixed
- Corrected prominent README examples to use the actual producer field,
  buffered transaction return values, verifier constructor, and experimental
  Moonshot clients; added compile-only Go examples for stable, experimental,
  embedded, and Testcontainers APIs
- Centralized SDK version `0.4.0` and use it for OpenTelemetry instrumentation
  scope metadata; release tags must now match the SDK version and changelog
- Authentication conformance now requires explicit modes and fixture inputs,
  removes hard-coded credentials and empty TLS configs, and fails instead of
  skipping when enabled infrastructure is missing
- Updated root `golang.org/x/net`, `x/text`, and `x/crypto` dependencies and the
  nested Testcontainers dependency graph to clear reachable `govulncheck`
  findings. The nested module now uses `golang.org/x/crypto@v0.56.0`, resolving
  GO-2026-6355 and GO-2026-6354 in the Testcontainers SSH transport.
- Corrected the security contact and current supported release line, and split
  general support from private vulnerability reporting
- `SECURITY.md` now reports to `security@streamlinelabs.dev`, matching the
  address used by every other repository in the organization, after an
  earlier pass in this branch had drifted to a different domain
- Removed the obsolete top-level `version` key from `docker-compose.test.yml`
  so `docker compose` no longer warns that it is ignored
- `NewClient` now applies the documented defaults to zero-valued `Config`
  fields, so a partial config such as `Config{Brokers: []string{"localhost:9092"}}`
  no longer fails with `kafka: invalid configuration (Net.DialTimeout must be > 0)`
- `Producer.Idempotent` now produces a valid Sarama configuration
  (single in-flight request) and reports a clear configuration error when it is
  combined with `RequiredAcks` other than `-1`
- `embedded` package: CGO bindings now match the vendored `streamline.h` C ABI
  (`streamline_open`/`streamline_close`/`streamline_produce`/`streamline_consume`)
  instead of symbols that do not exist
- Embedded consumption now reads `StreamlineRecordBatch.count` and frees every
  successful batch, matching the C ABI return contract
- `Config.TLS` is now applied to Sarama connections, including custom CA,
  mutual TLS, and the explicit `InsecureSkipVerify` option
- `examples/circuit_breaker`: use `Admin.CreateTopic(ctx, TopicConfig{...})`
- `examples/security`: use the current `TLSConfig` fields (`Enable`, `CAFile`,
  `CertFile`, `KeyFile`)
- `examples/agent_memory`: remove the redundant newline flagged by `go vet`
- Formatting drift across the SDK, examples, and testcontainers module (`gofmt`)
- Cleanup failures are no longer discarded. HTTP response bodies, Sarama offset
  managers, and the producer created during `NewClient` now report close errors,
  folded into the returned error only when the operation itself succeeded so a
  real failure is never masked
- Error responses from the HTTP admin, schema registry, query, and memory APIs
  no longer swallow a failure to read the response body
- Partition and replica counts from topic metadata, and schema IDs from the
  schema registry, are range-checked before being narrowed to the protocol's
  fixed-width types instead of silently overflowing
- `moonshot.ContractsClient.Validate` reports a decode error instead of
  returning an empty successful result when a `200` body cannot be parsed
- Examples run their body in a `run() error` function, so `defer client.Close()`
  is no longer skipped by `log.Fatalf`, and they report close failures
- Compiled example binaries are no longer tracked, and `.gitignore` now covers
  the binaries produced by building each `examples/` directory

### Changed
- Minimum Go version is now 1.25.14 for the root SDK. The nested
  `testcontainers/` module requires Go 1.26.0 because the fixed `x/crypto`
  release raised its minimum; CI and security scanning use patched Go 1.26.6
- Release automation is now a `workflow_dispatch` promotion flow from `main`
  instead of a tag-push trigger: promotion takes explicit `release_version`,
  `release_tag`, and an immutable `server_image_digest` input, verifies the
  selected commit is the current, reachable tip of `main`, runs the full
  root/nested build/vet/test/examples/`govulncheck` suite, starts a
  digest-pinned live server fixture and runs required conformance behind an
  executed-test-count guard, and only then generates the source archive,
  root/nested CycloneDX SBOMs, and checksums, signs and verifies them with
  keyless Cosign, and creates build provenance attestations. Creating and
  pushing the annotated tag and creating the GitHub release now happen in a
  separate job gated behind a protected `release-promotion` GitHub
  Environment, strictly after every check above has succeeded, so a tag is
  never published before the checks that are meant to gate it. Existing
  tag/version/changelog validation and the existing-release/tag-reuse guard
  are preserved (and now covered by focused script tests under
  `scripts/release/`)
- Release conformance now forces `go test -count=1`, and its executed-test
  guard rejects any `(cached)` package output. The regular integration
  workflow likewise requires the `STREAMLINE_INTEGRATION_IMAGE` repository
  variable to contain an immutable `repository@sha256` reference, validates
  the pulled digest, and has no mutable image-tag fallback.
- Dependabot and pinned `govulncheck` coverage now include the root and nested
  Testcontainers modules
- The `embedded` package requires the `embedded` build tag and CGO. Default
  builds compile a stub whose operations return `embedded.ErrNotEnabled`, so
  `go build ./...` and `go test ./...` no longer need `libstreamline`
- The conformance suite requires the `integration` build tag
  (`go test -tags=integration ./...`); `go test ./...` no longer contacts
  `localhost`. `STREAMLINE_SKIP_INTEGRATION=1` and `-short` skip it when it is
  compiled in
- `Producer.RequiredAcks: 0` retains Kafka's no-response/fire-and-forget
  semantics; use `DefaultConfig()` to select the SDK default of `-1`
- `Admin.GetConsumerGroupOffsets` and `Admin.ResetConsumerGroupOffsets` surface
  offset-manager close failures instead of ignoring them
- Dead code removed: the unused `validateResponse` helper in `streamline/client.go`
- `.golangci.yml` migrated to the golangci-lint v2 configuration schema, and the
  CI lint job bumped to `golangci-lint-action@v8` (v6 only supports v1 configs)

### Added
- Regression tests for configuration defaults and Sarama config validation
  (`streamline/config_defaults_test.go`)
- Tests for the live-server test selection helpers, runnable without a server
  (`testutil_selection_test.go`)
- Tests for the disabled and CGO-enabled `embedded` builds
- TLS wiring tests that generate certificate material on the fly, covering CA
  loading, mutual TLS, invalid PEM, and the disabled-TLS path
  (`streamline/tls_config_test.go`)
- Acknowledgement-level mapping tests (`0`/`1`/`-1` to the Sarama constants) and
  a test for the idempotent `RequiredAcks` promotion
- Tests for the shared cleanup helper and for `HTTPAdmin` error responses
  carrying the server body


## [0.3.0] - 2026-04-20

### Added
- `streamline/moonshot` package — HTTP clients for the Streamline Moonshot
  control plane (port `9094`):
  - `BranchesClient` (M5 — list / create / delete / merge branches)
  - `ContractsClient` (M4 — register / get / validate JSON-Schema contracts)
  - `AttestationClient` (M4 — request signatures, verify them)
  - `SearchClient` (M2 — semantic search across topics)
  - `MemoryClient` (M1 — agent memory remember / recall)
- `MoonshotOptions`, `MoonshotError` shared across the package.

### Added
- `HTTPAdmin` client for expanded admin operations via HTTP REST API
- `HTTPAdmin.ClusterInfo()` — cluster overview including broker list
- `HTTPAdmin.ConsumerGroupLag()` / `ConsumerGroupTopicLag()` — consumer group lag monitoring
- `HTTPAdmin.InspectMessages()` / `LatestMessages()` — message inspection by offset
- `HTTPAdmin.MetricsHistory()` — server metrics history
- Model types: `ClusterInfo`, `ConsumerLag`, `ConsumerGroupLag`, `InspectedMessage`, `MetricPoint`
- Unit tests for all HTTPAdmin methods using `httptest`

### Fixed
- `Consumer.Commit()` now actually commits offsets via Sarama session (was a no-op returning nil)
- `TracingProducer.SendAsync` span now ends after result is received (was ending before channel send)
- Consumer group handler now stores session reference for manual offset commits
- `HealthCheck()` now uses correct `Client.Admin` field and `Config.Brokers` (was referencing non-existent fields)

### Changed
- fix: handle context cancellation in consumer loop (2026-03-06)
- refactor: improve client connection management (2026-03-06)
- test: add benchmark for high-throughput producer (2026-03-06)
- **Fixed**: correct offset tracking in sync consumer
- **Documentation**: update README with consumer group examples
- **Added**: add TLS configuration helpers
- **Performance**: optimize message encoding buffer reuse
- **Fixed**: handle context cancellation in consumer loop
- **Changed**: update go.mod dependencies
- **Changed**: extract connection pool management
- **Testing**: add integration tests for producer batching
- **Fixed**: resolve panic on nil message handling
- **Added**: add consumer group support via Sarama wrapper

### Fixed
- Resolve race condition in consumer offset commit


## [0.2.0] - 2026-02-18

### Added
- `Client` with connection management and health checks
- `Producer` with sync and async (channel-based) message sending
- `Consumer` with poll-based message consumption and group support
- `Admin` client for topic and consumer group management
- Rich error types with retryability metadata and hints
- SASL/SCRAM authentication support
- TLS connection support
- Context support throughout all APIs
- Testcontainers integration for testing

### Infrastructure
- CI pipeline with go test, coverage reporting, and race detection
- govulncheck security scanning
- CodeQL security scanning
- Release workflow with Go module publishing
- Release drafter for automated release notes
- Dependabot for dependency updates
- CONTRIBUTING.md with development setup guide
- Security policy (SECURITY.md)
- EditorConfig for consistent formatting
- Issue templates for bug reports and feature requests

## [0.1.0] - 2026-02-18

### Added
- Initial release of Streamline Go SDK
- Built on IBM/sarama for Kafka protocol support
- Testcontainers support for integration testing
- Apache 2.0 license
- test: add health check endpoint integration test
- refactor: extract retry policy into standalone package
- docs: add gRPC metadata forwarding usage examples
- docs: add streaming consumer lifecycle documentation
- test: add metrics collection and health probe tests
