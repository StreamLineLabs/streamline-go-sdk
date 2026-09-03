#!/usr/bin/env bash
#
# Pulls the workflow_dispatch-provided server image strictly by its
# immutable digest and confirms the pulled image actually resolves to that
# exact digest before it is used to start the live conformance fixture.
#
# Validation (format, "@sha256:" presence, hex length) happens first and
# without touching the network, so a malformed input fails fast instead of
# spending a pull attempt.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/release/lib.sh
source "$SCRIPT_DIR/lib.sh"

: "${SERVER_IMAGE_DIGEST:?SERVER_IMAGE_DIGEST is required}"

validate_image_digest "$SERVER_IMAGE_DIGEST"

expected_digest="sha256:${SERVER_IMAGE_DIGEST#*@sha256:}"

release_log "pulling immutable server image $SERVER_IMAGE_DIGEST"
if ! docker pull "$SERVER_IMAGE_DIGEST"; then
	release_log "failed to pull $SERVER_IMAGE_DIGEST; the digest may not exist, may not be reachable, or the registry may be down"
	exit 1
fi

resolved="$(docker inspect --format='{{index .RepoDigests 0}}' "$SERVER_IMAGE_DIGEST" 2>/dev/null || true)"
if [[ -z "$resolved" ]]; then
	release_log "could not resolve a repo digest for the pulled image $SERVER_IMAGE_DIGEST"
	exit 1
fi

if [[ "$resolved" != *"$expected_digest" ]]; then
	release_log "pulled image digest ('$resolved') does not match the requested digest ('$SERVER_IMAGE_DIGEST')"
	exit 1
fi

release_log "verified server image resolves to the requested immutable digest: $resolved"
