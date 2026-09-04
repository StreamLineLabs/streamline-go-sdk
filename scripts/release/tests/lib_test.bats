#!/usr/bin/env bats
# Unit tests for scripts/release/lib.sh — pure functions only, no network,
# no Docker, no live git remote required.

setup() {
	REPO_ROOT="$(cd "$BATS_TEST_DIRNAME/../../.." && pwd)"
	# shellcheck source=scripts/release/lib.sh
	source "$REPO_ROOT/scripts/release/lib.sh"
}

# --- validate_semver ---------------------------------------------------

@test "validate_semver accepts a stable MAJOR.MINOR.PATCH version" {
	run validate_semver "0.3.0"
	[ "$status" -eq 0 ]
}

@test "validate_semver rejects an empty version" {
	run validate_semver ""
	[ "$status" -ne 0 ]
}

@test "validate_semver rejects a pre-release version" {
	run validate_semver "0.3.0-rc.1"
	[ "$status" -ne 0 ]
}

@test "validate_semver rejects a version with a leading zero segment" {
	run validate_semver "0.03.0"
	[ "$status" -ne 0 ]
}

@test "validate_semver rejects a v-prefixed version" {
	run validate_semver "v0.3.0"
	[ "$status" -ne 0 ]
}

# --- validate_tag_for_version ------------------------------------------

@test "validate_tag_for_version accepts a matching tag" {
	run validate_tag_for_version "v0.3.0" "0.3.0"
	[ "$status" -eq 0 ]
}

@test "validate_tag_for_version rejects a mismatched tag" {
	run validate_tag_for_version "v0.3.1" "0.3.0"
	[ "$status" -ne 0 ]
}

@test "validate_tag_for_version rejects a tag missing the v prefix" {
	run validate_tag_for_version "0.3.0" "0.3.0"
	[ "$status" -ne 0 ]
}

@test "validate_tag_for_version rejects an empty tag" {
	run validate_tag_for_version "" "0.3.0"
	[ "$status" -ne 0 ]
}

# --- validate_image_digest ----------------------------------------------

@test "validate_image_digest accepts a well-formed sha256 digest reference" {
	run validate_image_digest "ghcr.io/streamlinelabs/streamline@sha256:$(printf 'a%.0s' {1..64})"
	[ "$status" -eq 0 ]
}

@test "validate_image_digest rejects a bare mutable tag" {
	run validate_image_digest "ghcr.io/streamlinelabs/streamline:0.3.0"
	[ "$status" -ne 0 ]
	[[ "$output" == *"@sha256:"* ]]
}

@test "validate_image_digest rejects a tag+digest reference disguised without @" {
	run validate_image_digest "ghcr.io/streamlinelabs/streamline:latest"
	[ "$status" -ne 0 ]
}

@test "validate_image_digest rejects a digest that is too short" {
	run validate_image_digest "ghcr.io/streamlinelabs/streamline@sha256:abc123"
	[ "$status" -ne 0 ]
}

@test "validate_image_digest rejects uppercase hex in the digest" {
	run validate_image_digest "ghcr.io/streamlinelabs/streamline@sha256:$(printf 'A%.0s' {1..64})"
	[ "$status" -ne 0 ]
}

@test "validate_image_digest rejects a digest algorithm other than sha256" {
	run validate_image_digest "ghcr.io/streamlinelabs/streamline@sha512:$(printf 'a%.0s' {1..128})"
	[ "$status" -ne 0 ]
}

@test "validate_image_digest rejects an empty reference" {
	run validate_image_digest ""
	[ "$status" -ne 0 ]
}

@test "validate_image_digest rejects a reference containing whitespace" {
	run validate_image_digest "ghcr.io/streamlinelabs/streamline @sha256:$(printf 'a%.0s' {1..64})"
	[ "$status" -ne 0 ]
}

@test "validate_image_digest rejects a reference with two @ characters" {
	run validate_image_digest "ghcr.io/streamlinelabs/streamline@evil@sha256:$(printf 'a%.0s' {1..64})"
	[ "$status" -ne 0 ]
}

@test "validate_image_digest rejects a missing repository before the digest" {
	run validate_image_digest "@sha256:$(printf 'a%.0s' {1..64})"
	[ "$status" -ne 0 ]
}

# --- sdk_version_from_source --------------------------------------------

@test "sdk_version_from_source reads the Version constant" {
	tmp="$(mktemp -d)"
	cat >"$tmp/version.go" <<'EOF'
package streamline

const Version = "1.2.3"
EOF
	run sdk_version_from_source "$tmp/version.go"
	[ "$status" -eq 0 ]
	[ "$output" = "1.2.3" ]
	rm -rf "$tmp"
}

@test "sdk_version_from_source fails when the file is missing" {
	run sdk_version_from_source "/nonexistent/version.go"
	[ "$status" -ne 0 ]
}

@test "sdk_version_from_source fails when no Version constant is present" {
	tmp="$(mktemp -d)"
	cat >"$tmp/version.go" <<'EOF'
package streamline
EOF
	run sdk_version_from_source "$tmp/version.go"
	[ "$status" -ne 0 ]
	rm -rf "$tmp"
}

# --- changelog helpers ---------------------------------------------------

@test "changelog_has_release_section finds an existing version heading" {
	tmp="$(mktemp -d)"
	cat >"$tmp/CHANGELOG.md" <<'EOF'
# Changelog

## [Unreleased]

## [0.3.0]

- Something released.
EOF
	run changelog_has_release_section "$tmp/CHANGELOG.md" "0.3.0"
	[ "$status" -eq 0 ]
	rm -rf "$tmp"
}

@test "changelog_has_release_section fails when the version heading is absent" {
	tmp="$(mktemp -d)"
	cat >"$tmp/CHANGELOG.md" <<'EOF'
# Changelog

## [Unreleased]
EOF
	run changelog_has_release_section "$tmp/CHANGELOG.md" "0.3.0"
	[ "$status" -ne 0 ]
	rm -rf "$tmp"
}

@test "changelog_has_pending_unreleased detects pending bullet entries" {
	tmp="$(mktemp -d)"
	cat >"$tmp/CHANGELOG.md" <<'EOF'
# Changelog

## [Unreleased]

### Fixed
- Something not yet released.

## [0.3.0]
EOF
	run changelog_has_pending_unreleased "$tmp/CHANGELOG.md"
	[ "$status" -eq 0 ]
	rm -rf "$tmp"
}

@test "changelog_has_pending_unreleased passes when Unreleased is empty" {
	tmp="$(mktemp -d)"
	cat >"$tmp/CHANGELOG.md" <<'EOF'
# Changelog

## [Unreleased]

## [0.3.0]

- Released entry.
EOF
	run changelog_has_pending_unreleased "$tmp/CHANGELOG.md"
	[ "$status" -ne 0 ]
	rm -rf "$tmp"
}

# --- expected_conformance_test_count -------------------------------------

@test "expected_conformance_test_count counts non-auth conformance tests and excludes auth tests" {
	tmp="$(mktemp -d)"
	cat >"$tmp/conformance_test.go" <<'EOF'
package streamline_test

func TestP01_SimpleProduce(t *testing.T) {}
func TestP02_KeyedProduce(t *testing.T) {}
func TestA01_TLSConnect(t *testing.T) {}
func TestA02_MutualTLS(t *testing.T) {}
func TestD01_CreateTopic(t *testing.T) {}
EOF
	run expected_conformance_test_count "$tmp/conformance_test.go"
	[ "$status" -eq 0 ]
	[ "$output" = "3" ]
	rm -rf "$tmp"
}

@test "expected_conformance_test_count against the real conformance suite is positive and excludes auth tests" {
	run expected_conformance_test_count "$REPO_ROOT/conformance_test.go"
	[ "$status" -eq 0 ]
	[ "$output" -gt 0 ]
	auth_count="$(grep -cE '^func TestA[0-9]{2}_' "$REPO_ROOT/conformance_test.go")"
	total_count="$(grep -cE '^func Test[PCGADSEF][0-9]{2}_' "$REPO_ROOT/conformance_test.go")"
	[ "$output" -eq "$((total_count - auth_count))" ]
}

# --- tag existence helpers (real local git fixtures, no network) --------

@test "tag_exists_local detects a tag created in the current repo" {
	tmp="$(mktemp -d)"
	git -C "$tmp" init -q
	git -C "$tmp" -c user.email=test@example.com -c user.name=test commit -q --allow-empty -m init
	git -C "$tmp" tag v9.9.9
	cd "$tmp"
	run tag_exists_local "v9.9.9"
	[ "$status" -eq 0 ]
	run tag_exists_local "v0.0.1-does-not-exist"
	[ "$status" -ne 0 ]
	rm -rf "$tmp"
}

@test "tag_exists_remote detects a tag on a local bare 'remote' repo" {
	remote="$(mktemp -d)"
	work="$(mktemp -d)"
	git init -q --bare "$remote"
	git clone -q "$remote" "$work"
	git -C "$work" -c user.email=test@example.com -c user.name=test commit -q --allow-empty -m init
	git -C "$work" tag v9.9.9
	git -C "$work" push -q origin v9.9.9

	cd "$work"
	run tag_exists_remote "origin" "v9.9.9"
	[ "$status" -eq 0 ]
	run tag_exists_remote "origin" "v0.0.1-does-not-exist"
	[ "$status" -ne 0 ]
	rm -rf "$remote" "$work"
}

# --- release_status_ok ---------------------------------------------------

@test "release_status_ok treats 404 as safe to proceed" {
	run release_status_ok "404"
	[ "$status" -eq 0 ]
}

@test "release_status_ok blocks on 200 (release already exists)" {
	run release_status_ok "200"
	[ "$status" -ne 0 ]
}

@test "release_status_ok blocks on an unexpected status" {
	run release_status_ok "503"
	[ "$status" -ne 0 ]
}
