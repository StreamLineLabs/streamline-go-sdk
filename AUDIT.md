# Clean Code and SRP Audit

## Summary

- **Highest-leverage split:** restore the documented `streamline/config.go`
  boundary by moving configuration models, defaults, TLS/SASL translation, and
  Sarama construction out of `client.go`.
- `client.go` currently changes for public configuration policy, TLS/security,
  Sarama construction, client lifecycle, and health/admin helpers.
- Producer/consumer concurrency classes share state coherently and should not be
  split without stronger race/timing characterization.
- The baseline repair already separated embedded/native and integration actors
  through explicit build tags.
- Admin and Moonshot files are long but each public method group shares one
  backend actor; route changes remain contract-gated.

## Findings

| ID | Location | Category | Severity | Actors in conflict | Cost | Size | Behavior risk |
|---|---|---|---|---|---|---|---|
| GO-SRP-1 | `streamline/client.go` | SRP, mixed module | P1 | config/product defaults; security/TLS/SASL; Sarama adapter; client lifecycle | Security/config changes and client lifecycle changes edit one 600-line file and obscure defaults. | L | Low |
| GO-SRP-2 | `streamline/admin.go` | Mixed admin surface | P2 | Kafka admin; HTTP admin/observability; offset reset policy | Different protocols and operator actors share one public type, but method state is thin and splitting could add forwarding layers. | L | Medium |
| GO-CC-1 | `streamline/moonshot/moonshot.go` | Repeated HTTP mechanics | P2 | branches/contracts/attestation/search product actors; HTTP transport | Endpoint policies and shared request mechanics coexist in one large module. | L | Medium |

## Ordered Refactor Sequence

1. Use the existing default/TLS/SASL/acks characterization tests.
2. Move all configuration types/defaults and Sarama translation unchanged into
   `config.go`.
3. Keep `Client`, `NewClient`, health, close, and child-client factories in
   `client.go`.
4. Validate both root and testcontainers modules plus build-tag variants.
5. Defer producer/consumer/admin splits until concurrency and callback behavior
   is characterized.

## Deferred

- Admin protocol split may create `Admin -> client -> transport` forwarding and
  is deferred.
- Moonshot HTTP centralization requires endpoint characterization.
- Embedded runtime tests need a real `libstreamline`.

## Out of Scope

- `Producer`, `Consumer`, and circuit breaker: stateful cohesive actors.
- Error taxonomy: public compatibility surface.
- Testcontainers module: independent deployment-test product.
