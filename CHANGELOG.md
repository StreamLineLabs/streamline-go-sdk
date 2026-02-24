# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
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
