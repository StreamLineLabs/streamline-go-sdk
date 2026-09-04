# shellcheck shell=bash
#
# Shared, pure(ish) helper functions for the release-promotion workflow.
#
# These functions are intentionally split out of the workflow YAML so they
# can be unit tested with bats (see scripts/release/tests/) without needing
# a GitHub Actions runner, Docker, or network access. Functions that must
# talk to git/curl/docker take their inputs as arguments (or accept an
# injected command) so tests can supply local fixtures instead of live
# infrastructure.
#
# Intended usage: `source scripts/release/lib.sh` from another script or
# workflow step. This file must not have side effects when sourced.

# A stable semantic version: MAJOR.MINOR.PATCH, no leading zeros, no
# pre-release/build metadata. Mirrors semanticVersionPattern in
# streamline/version_test.go so both layers agree on what "releasable"
# means.
release_semver_pattern='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'

release_log() {
	printf 'release: %s\n' "$*" >&2
}

# validate_semver <version>
# Fails unless <version> is a stable MAJOR.MINOR.PATCH string.
validate_semver() {
	local version="$1"
	if [[ -z "$version" ]]; then
		release_log "version must not be empty"
		return 1
	fi
	if [[ ! "$version" =~ $release_semver_pattern ]]; then
		release_log "version '$version' is not a stable semantic version (want MAJOR.MINOR.PATCH)"
		return 1
	fi
	return 0
}

# validate_tag_for_version <tag> <version>
# Fails unless <tag> is exactly "v<version>".
validate_tag_for_version() {
	local tag="$1" version="$2" want
	want="v${version}"
	if [[ -z "$tag" ]]; then
		release_log "release tag must not be empty"
		return 1
	fi
	if [[ "$tag" != "$want" ]]; then
		release_log "release tag '$tag' does not match expected tag '$want' for version '$version'"
		return 1
	fi
	return 0
}

# validate_image_digest <ref>
# Fails unless <ref> pins an image by an explicit, immutable sha256 digest
# (repository@sha256:<64 lowercase hex chars>). A bare tag (e.g. ":0.3.0" or
# ":latest") is mutable and is rejected even if it happens to also be
# present in the reference.
validate_image_digest() {
	local ref="$1" repo digest

	if [[ -z "$ref" ]]; then
		release_log "server image reference must not be empty"
		return 1
	fi
	if [[ "$ref" == *[[:space:]]* ]]; then
		release_log "server image reference must not contain whitespace: '$ref'"
		return 1
	fi
	if [[ "$ref" != *"@sha256:"* ]]; then
		release_log "server image reference must be pinned with '@sha256:<64-hex-digest>', got: '$ref'"
		return 1
	fi

	repo="${ref%%@sha256:*}"
	digest="${ref#*@sha256:}"

	if [[ -z "$repo" ]]; then
		release_log "server image reference is missing a repository before the digest: '$ref'"
		return 1
	fi
	if [[ "$repo" == *"@"* ]]; then
		release_log "server image reference must contain exactly one '@': '$ref'"
		return 1
	fi
	if [[ ! "$digest" =~ ^[0-9a-f]{64}$ ]]; then
		release_log "server image digest must be exactly 64 lowercase hex characters after 'sha256:', got: '$digest'"
		return 1
	fi
	return 0
}

# sdk_version_from_source <path/to/version.go>
# Extracts the value of `const Version = "X.Y.Z"` from the Go source file.
sdk_version_from_source() {
	local file="$1" version
	if [[ ! -f "$file" ]]; then
		release_log "SDK version source file not found: '$file'"
		return 1
	fi
	version="$(grep -oE '^const Version = "[^"]+"' "$file" | grep -oE '"[^"]+"' | tr -d '"')"
	if [[ -z "$version" ]]; then
		release_log "could not find 'const Version = \"...\"' in '$file'"
		return 1
	fi
	printf '%s' "$version"
	return 0
}

# changelog_has_release_section <changelog-file> <version>
# Succeeds when the changelog has a "## [<version>]" heading.
changelog_has_release_section() {
	local file="$1" version="$2"
	if [[ ! -f "$file" ]]; then
		release_log "changelog file not found: '$file'"
		return 1
	fi
	grep -Eq "^## \[$version\]( |$)" "$file"
}

# changelog_has_pending_unreleased <changelog-file>
# Succeeds (exit 0) when an "## [Unreleased]" section exists and still has
# entries under it. This mirrors the awk logic previously inlined in
# release.yml so the release must not publish while unreleased notes exist.
changelog_has_pending_unreleased() {
	local file="$1"
	if [[ ! -f "$file" ]]; then
		release_log "changelog file not found: '$file'"
		return 1
	fi
	awk '
        /^## \[Unreleased\]/ { in_unreleased = 1; next }
        /^## \[/ { in_unreleased = 0 }
        in_unreleased && (/^### / || /^- /) { found = 1 }
        END { exit found ? 0 : 1 }
    ' "$file"
}

# expected_conformance_test_count <conformance-test-file>
# Counts the required (non-auth) conformance test functions so the
# executed-test-count guard has a threshold that tracks the suite instead of
# a hand-maintained magic number. Auth ("TestA##_...") tests are excluded
# because they legitimately skip without externally provisioned auth
# fixture secrets, which the promotion fixture does not provide (auth has
# its own dedicated conformance workflow).
expected_conformance_test_count() {
	local file="$1"
	if [[ ! -f "$file" ]]; then
		release_log "conformance test file not found: '$file'"
		return 1
	fi
	grep -cE '^func Test[PCGDSEF][0-9]{2}_' "$file"
}

# tag_exists_local <tag>
tag_exists_local() {
	local tag="$1"
	git show-ref --tags --quiet -- "refs/tags/$tag"
}

# tag_exists_remote <remote> <tag>
tag_exists_remote() {
	local remote="$1" tag="$2" out
	out="$(git ls-remote --tags "$remote" "refs/tags/$tag" 2>/dev/null)"
	[[ -n "$out" ]]
}

# release_status_ok <http-status-code>
# Interprets the GitHub "get release by tag" HTTP status. Pure function so
# it can be tested without a network call. 404 means no such release exists
# (ok to proceed); anything else (200 = exists, or an error status) blocks
# promotion so we never silently overwrite or misreport release state.
release_status_ok() {
	local code="$1"
	[[ "$code" == "404" ]]
}

# fetch_release_status <api-url> <owner/repo> <tag> <token>
# Thin wrapper around the GitHub API call. Kept separate from
# release_status_ok so the decision logic can be unit tested without
# network access.
fetch_release_status() {
	local api_url="$1" repo="$2" tag="$3" token="$4"
	curl \
		--silent \
		--show-error \
		--output /dev/null \
		--write-out '%{http_code}' \
		--header "Authorization: Bearer ${token}" \
		--header "Accept: application/vnd.github+json" \
		--header "X-GitHub-Api-Version: 2022-11-28" \
		"$api_url/repos/$repo/releases/tags/$tag"
}
