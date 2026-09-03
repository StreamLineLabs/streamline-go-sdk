# Release Readiness Audit

**Audit date:** 2026-09-02

**Branch reviewed:** `refactor/clean-code-srp`

## Status

All safe programmatic P0/P1 release-readiness fixes identified in this
repository have been implemented.

Release publication is still gated on external infrastructure checks that
cannot be reproduced in this worktree: a running Docker daemon for the standard
conformance fixture, an externally provisioned auth-enabled Streamline server,
the native `libstreamline` library, and GitHub Actions OIDC/release permissions.

## Completed P0/P1 Work

| Area | Result |
| --- | --- |
| Public examples | Corrected invalid producer, transaction, verifier, memory, and branch examples. Added compile-only Go examples for stable APIs, Moonshot APIs, embedded mode, and the nested Testcontainers module. |
| Security/support | Corrected the security contact to `security@streamlinelabs.dev` (matching every other repository in the organization; an earlier pass in this worktree had drifted to `security@streamline.dev`), added private advisory reporting, updated support to 0.4.x, and separated normal support from vulnerability reporting. |
| Auth conformance | Removed hard-coded credentials and empty TLS configs. Auth runs require explicit modes and mode-specific inputs and fail closed when enabled. Added a manual workflow for an externally managed auth fixture. |
| Integration selection | Added `STREAMLINE_REQUIRE_INTEGRATION`; CI and Makefile live-test paths fail when the required server is unavailable instead of silently skipping. |
| Release supply chain | Release tags must match the SDK version, have a changelog section, leave no pending `[Unreleased]` entries, not already have a GitHub release, and point to a commit reachable from `main`. The workflow tests both modules, creates root/nested CycloneDX SBOMs, generates checksums, signs and verifies the checksum manifest with keyless Cosign, and creates GitHub provenance attestations. |
| Dependency automation | Dependabot covers GitHub Actions plus both Go modules. Actions and release/security tools use explicit versions. |
| Vulnerability remediation | Updated root `x/net`, `x/text`, and `x/crypto`; upgraded Testcontainers from 0.27.0 to 0.44.0; overrode `moby/go-archive` to the fixed 0.3.0 release; and updated the nested module to `golang.org/x/crypto` 0.56.0 to resolve GO-2026-6354 and GO-2026-6355. |
| Go support | The root SDK requires Go 1.25.14 or later. The nested Testcontainers module requires Go 1.26.0 because `x/crypto` 0.56.0 raised its minimum, and CI/security scans use patched Go 1.26.6. |

## Verification Performed

The following checks completed successfully with `GOTOOLCHAIN=go1.25.14` for
the root module and `GOTOOLCHAIN=go1.26.6` for the nested Testcontainers module
unless noted otherwise:

- `go fmt ./...` in the root and nested Testcontainers modules
- `go mod tidy` and `go mod verify` in both modules
- `go build ./...` in both modules
- `go vet ./...` in both modules
- `go test -count=1 ./...` in both modules
- `go vet -tags=integration ./...`
- `go test -short -count=1 -tags=integration ./...`
- `go build ./examples/...`
- Compile-only Go example coverage in root, Moonshot, embedded, and
  Testcontainers packages
- `CGO_ENABLED=0 go build -tags=embedded ./...` for the supported embedded
  fallback
- `govulncheck` v1.7.0 for both modules: zero reachable vulnerabilities
- `golangci-lint` v2.12.2: zero issues; CI is pinned to v2.13.2
- `actionlint`: all GitHub workflows pass
- YAML parsing for GitHub configuration and JSON parsing for the devcontainer
- Negative auth selection check: enabling auth without
  `STREAMLINE_AUTH_MODES` failed as required
- Remote tag inspection confirmed that `v0.3.0` already exists, so this
  uncommitted work must not move or reuse it

`govulncheck` still reports advisories in transitive modules whose vulnerable
symbols are not reachable from this code. Dependabot is configured for both
modules so those dependencies continue to receive update proposals.

## External Blockers and Unrun Checks

### Standard live conformance

Not run locally. The Docker CLI could not obtain daemon information and panicked
while formatting an empty server response, so the Compose fixture was not
usable in this environment. Tagged compilation, vet, selection tests, and
fail-closed workflow wiring passed.

### Authentication conformance

Not run. Streamline authentication is feature-gated and requires an
auth-enabled server build, users file, credentials, and TLS material. None are
present in this repository, and fabricating an auth fixture would not validate
the real server. The manual `Auth Conformance` workflow documents and enforces
the required protected-environment inputs.

### Native embedded mode

Not run. The `embedded && cgo` build requires an externally installed,
ABI-compatible `libstreamline`. The CGO-disabled fallback builds and its unit
tests pass.

### Signing, provenance, and publication

Not run. Keyless Cosign signing, GitHub artifact attestations, and release
creation require a real approved tag on `main` plus GitHub OIDC and repository
permissions. The workflow is fail-fast and was validated with YAML parsing and
`actionlint`. It now also rejects an existing GitHub release and refuses to
publish while `[Unreleased]` contains entries. No SBOM, signature, attestation,
or release result is claimed until that workflow succeeds.

## Required Release Follow-up

1. Merge the reviewed work to `main`.
2. Run the normal integration workflow on a runner with a working Docker
   daemon and confirm the pinned server fixture passes.
3. Provision the protected `auth-conformance` environment and run every
   supported auth mode against the real auth-enabled server fixture.
4. If native embedded support is part of the release promise, build and test it
   against the exact `libstreamline` artifact to be supported.
5. Do not move or reuse the existing `v0.3.0` tag. Publish only from a new
   release tag after moving the changelog entries into that release section
   and require the signing, SBOM, provenance, and publication workflow to
   complete without overrides.

## Follow-up Verification Pass (2026-09-03)

A later session in the same worktree re-ran the full local gate set and found
the Docker daemon reachable this time (`docker info` succeeded and `docker
pull hello-world` confirmed general registry egress). With a real daemon
available, `make integration-test` was actually attempted rather than assumed
blocked:

- `docker pull ghcr.io/streamlinelabs/streamline:0.3.0` and `:latest` both
  fail with `manifest unknown`; unauthenticated `GET
  /v2/streamlinelabs/streamline/tags/list` against `ghcr.io` returns
  `UNAUTHORIZED`. The package is not publicly pullable from this environment
  (private package or no anonymous access), independent of any Docker
  installation issue. `make integration-test` fails closed with a non-zero
  exit as designed (`Error 18`) instead of hanging or silently passing — the
  gate itself needed no changes.
- Re-ran `go build/vet/test`, `golangci-lint run`, `go build ./examples/...`,
  and `make examples` in this pass: all still pass with zero issues.
- Updated the nested module from `golang.org/x/crypto` 0.54.0 to 0.56.0,
  resolving reachable GO-2026-6355 and GO-2026-6354 findings in the
  Testcontainers SSH transport. Because `x/crypto` 0.56.0 requires Go 1.26,
  the nested module's `go` directive is now 1.26.0, its `toolchain` directive
  selects patched Go 1.26.6, and its CI, release, and Makefile paths use that
  toolchain. Nested build, vet, tests, module verification, and
  `govulncheck` now pass with zero reachable vulnerabilities.
- Corrected `SECURITY.md`'s reporting email back to `security@streamlinelabs.dev`
  (see the Security/support row above).
- Removed the obsolete top-level `version: "3.8"` key from
  `docker-compose.test.yml` (Compose v2 warns and ignores it); `docker compose
  ... config` now produces no warnings.

No SDK or companion-package release version changed. The nested dependency and
minimum Go toolchain were updated for security, and none of these changes are
committed by this pass.
