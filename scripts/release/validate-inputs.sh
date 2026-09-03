#!/usr/bin/env bash
#
# Validates the workflow_dispatch promotion inputs before anything else in
# the release-promotion workflow runs: the release version/tag pair, the
# explicit immutable server image digest, and that the changelog is ready
# to publish. This is pure input validation — it does not touch git remotes,
# Docker, or the GitHub API (see verify-source.sh for those checks) — so it
# can run first, fast, and be unit tested offline.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/release/lib.sh
source "$SCRIPT_DIR/lib.sh"

: "${RELEASE_VERSION:?RELEASE_VERSION is required (workflow_dispatch input release_version)}"
: "${RELEASE_TAG:?RELEASE_TAG is required (workflow_dispatch input release_tag)}"
: "${SERVER_IMAGE_DIGEST:?SERVER_IMAGE_DIGEST is required (workflow_dispatch input server_image_digest)}"
SDK_VERSION_FILE="${SDK_VERSION_FILE:-streamline/version.go}"
CHANGELOG_FILE="${CHANGELOG_FILE:-CHANGELOG.md}"

sdk_version="$(sdk_version_from_source "$SDK_VERSION_FILE")"

validate_semver "$RELEASE_VERSION"
validate_semver "$sdk_version"

if [[ "$RELEASE_VERSION" != "$sdk_version" ]]; then
	release_log "release_version input ('$RELEASE_VERSION') does not match the SDK Version constant ('$sdk_version') in $SDK_VERSION_FILE"
	exit 1
fi

validate_tag_for_version "$RELEASE_TAG" "$RELEASE_VERSION"
validate_image_digest "$SERVER_IMAGE_DIGEST"

if ! changelog_has_release_section "$CHANGELOG_FILE" "$RELEASE_VERSION"; then
	release_log "$CHANGELOG_FILE has no release section for $RELEASE_VERSION"
	exit 1
fi

if changelog_has_pending_unreleased "$CHANGELOG_FILE"; then
	release_log "$CHANGELOG_FILE still contains unreleased entries; cut a new version section before promoting"
	exit 1
fi

release_log "promotion inputs valid: version=$RELEASE_VERSION tag=$RELEASE_TAG digest=$SERVER_IMAGE_DIGEST"
