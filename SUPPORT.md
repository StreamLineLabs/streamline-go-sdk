# Support Policy

## Where to Get Help

- **SDK bugs:** Open a
  [bug report](https://github.com/streamlinelabs/streamline-go-sdk/issues/new?template=bug_report.yml).
- **SDK feature requests:** Open a
  [feature request](https://github.com/streamlinelabs/streamline-go-sdk/issues/new?template=feature_request.yml).
- **Usage and ecosystem questions:** Use
  [Streamline Discussions](https://github.com/streamlinelabs/streamline/discussions).
- **Security vulnerabilities:** Follow [SECURITY.md](SECURITY.md); never post
  vulnerability details in a public issue or discussion.

Before opening a report, search existing issues and confirm the problem still
occurs with the latest 0.4.x release and a supported Go version.

## Supported Environment

- Streamline Go SDK 0.4.x
- Go 1.25.14 or later, with current security patches
- Streamline server 0.2.0 or later for the stable Kafka-compatible APIs
- Streamline server 0.3.0 with the required feature flags for experimental
  Moonshot APIs

Older SDK lines may still work, but they do not receive routine fixes.

## Information to Include

- SDK, Go, and Streamline server versions
- Operating system and architecture
- A minimal reproducible example
- Relevant logs and configuration with all secrets removed
- Whether the issue requires Docker, CGO, authentication, or an experimental
  server feature

Community support is best effort and has no guaranteed response or resolution
time.
