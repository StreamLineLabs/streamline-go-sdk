#!/usr/bin/env bats
# End-to-end tests for scripts/release/validate-inputs.sh using disposable
# fixture files (no network, no live repo state mutated).

setup() {
	REPO_ROOT="$(cd "$BATS_TEST_DIRNAME/../../.." && pwd)"
	SCRIPT="$REPO_ROOT/scripts/release/validate-inputs.sh"
	TMP="$(mktemp -d)"

	cat >"$TMP/version.go" <<'EOF'
package streamline

const Version = "0.3.0"
EOF

	cat >"$TMP/CHANGELOG.md" <<'EOF'
# Changelog

## [Unreleased]

## [0.3.0]

- Released entry.

## [0.2.0]

- Older entry.
EOF
}

teardown() {
	rm -rf "$TMP"
}

run_validate() {
	SDK_VERSION_FILE="$TMP/version.go" CHANGELOG_FILE="$TMP/CHANGELOG.md" \
		RELEASE_VERSION="${1-}" RELEASE_TAG="${2-}" SERVER_IMAGE_DIGEST="${3-}" \
		bash "$SCRIPT"
}

@test "validate-inputs.sh accepts a fully consistent set of inputs" {
	run run_validate "0.3.0" "v0.3.0" "ghcr.io/streamlinelabs/streamline@sha256:$(printf 'a%.0s' {1..64})"
	[ "$status" -eq 0 ]
	[[ "$output" == *"promotion inputs valid"* ]]
}

@test "validate-inputs.sh rejects release_version that does not match the SDK Version constant" {
	run run_validate "0.4.0" "v0.4.0" "ghcr.io/streamlinelabs/streamline@sha256:$(printf 'a%.0s' {1..64})"
	[ "$status" -ne 0 ]
	[[ "$output" == *"does not match the SDK Version constant"* ]]
}

@test "validate-inputs.sh rejects a tag that does not match release_version" {
	run run_validate "0.3.0" "v0.3.1" "ghcr.io/streamlinelabs/streamline@sha256:$(printf 'a%.0s' {1..64})"
	[ "$status" -ne 0 ]
}

@test "validate-inputs.sh rejects a mutable tag reference for the server image" {
	run run_validate "0.3.0" "v0.3.0" "ghcr.io/streamlinelabs/streamline:0.3.0"
	[ "$status" -ne 0 ]
	[[ "$output" == *"@sha256:"* ]]
}

@test "validate-inputs.sh rejects a missing server_image_digest input" {
	run bash -c "SDK_VERSION_FILE='$TMP/version.go' CHANGELOG_FILE='$TMP/CHANGELOG.md' RELEASE_VERSION=0.3.0 RELEASE_TAG=v0.3.0 bash '$SCRIPT'"
	[ "$status" -ne 0 ]
}

@test "validate-inputs.sh fails closed when CHANGELOG has no section for the version" {
	cat >"$TMP/CHANGELOG.md" <<'EOF'
# Changelog

## [Unreleased]
EOF
	run run_validate "0.3.0" "v0.3.0" "ghcr.io/streamlinelabs/streamline@sha256:$(printf 'a%.0s' {1..64})"
	[ "$status" -ne 0 ]
	[[ "$output" == *"no release section"* ]]
}

@test "validate-inputs.sh fails closed when CHANGELOG still has pending Unreleased entries" {
	cat >"$TMP/CHANGELOG.md" <<'EOF'
# Changelog

## [Unreleased]

### Fixed
- Not yet released.

## [0.3.0]

- Released entry.
EOF
	run run_validate "0.3.0" "v0.3.0" "ghcr.io/streamlinelabs/streamline@sha256:$(printf 'a%.0s' {1..64})"
	[ "$status" -ne 0 ]
	[[ "$output" == *"unreleased entries"* ]]
}
