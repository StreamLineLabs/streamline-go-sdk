#!/usr/bin/env bats
# Tests for scripts/release/verify-server-image.sh.
#
# Format validation must fail before any network/Docker access is
# attempted, so those cases run unconditionally. The "hard block on an
# unreachable/nonexistent digest" case additionally requires Docker and
# outbound network access to a registry; it is skipped (not silently
# passed) when neither is available, and clearly reports why.

setup() {
	REPO_ROOT="$(cd "$BATS_TEST_DIRNAME/../../.." && pwd)"
	SCRIPT="$REPO_ROOT/scripts/release/verify-server-image.sh"
}

@test "verify-server-image.sh fails fast on a malformed digest without calling docker" {
	# A `docker` shim that fails the test if invoked proves validation runs
	# before any pull attempt.
	stub_dir="$(mktemp -d)"
	cat >"$stub_dir/docker" <<'EOF'
#!/usr/bin/env bash
echo "docker should not have been invoked for a malformed digest" >&2
exit 1
EOF
	chmod +x "$stub_dir/docker"

	run env PATH="$stub_dir:$PATH" SERVER_IMAGE_DIGEST="ghcr.io/streamlinelabs/streamline:0.3.0" bash "$SCRIPT"
	[ "$status" -ne 0 ]
	[[ "$output" == *"@sha256:"* ]]

	rm -rf "$stub_dir"
}

@test "verify-server-image.sh requires SERVER_IMAGE_DIGEST to be set" {
	run bash "$SCRIPT"
	[ "$status" -ne 0 ]
}

@test "verify-server-image.sh hard-blocks a well-formed but nonexistent/unreachable digest" {
	if ! command -v docker >/dev/null 2>&1; then
		skip "docker is not installed in this environment"
	fi
	if ! docker info >/dev/null 2>&1; then
		skip "docker daemon is not reachable in this environment"
	fi

	# This is a syntactically valid digest that does not exist on the
	# registry (there is no published streamline server image at this
	# digest). No real "live fixture" digest is available in this
	# environment/registry, so this validates the required hard-block path
	# instead of the live positive path.
	digest="ghcr.io/streamlinelabs/streamline@sha256:$(printf '0%.0s' {1..64})"
	run env SERVER_IMAGE_DIGEST="$digest" bash "$SCRIPT"
	[ "$status" -ne 0 ]
	[[ "$output" == *"failed to pull"* ]]
}
