#!/usr/bin/env bash
#
# Verifies that the commit selected for promotion is releasable:
#   * it is the current tip of origin/main (not stale, not another branch),
#   * it is reachable from origin/main (defense in depth alongside the
#     equality check above),
#   * the release tag does not already exist locally or on the remote, and
#   * no GitHub release already exists for the tag.
#
# This must run against a checkout with `fetch-depth: 0` (or at least a
# fetched origin/main) so merge-base/ls-remote have the history they need.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/release/lib.sh
source "$SCRIPT_DIR/lib.sh"

: "${RELEASE_TAG:?RELEASE_TAG is required}"
: "${TARGET_SHA:?TARGET_SHA is required (the commit under promotion, e.g. GITHUB_SHA)}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
: "${GITHUB_TOKEN:?GITHUB_TOKEN is required}"
MAIN_REF="${MAIN_REF:-origin/main}"
GITHUB_API_URL="${GITHUB_API_URL:-https://api.github.com}"
REMOTE="${REMOTE:-origin}"

if ! git rev-parse --verify --quiet "$MAIN_REF" >/dev/null; then
	release_log "could not resolve '$MAIN_REF'; fetch main before verifying the source commit"
	exit 1
fi

main_sha="$(git rev-parse "$MAIN_REF")"

if ! git merge-base --is-ancestor "$TARGET_SHA" "$MAIN_REF"; then
	release_log "commit $TARGET_SHA is not reachable from $MAIN_REF"
	exit 1
fi

if [[ "$TARGET_SHA" != "$main_sha" ]]; then
	release_log "commit $TARGET_SHA is not the current tip of $MAIN_REF ($main_sha); dispatch promotion again from the latest main"
	exit 1
fi

if tag_exists_local "$RELEASE_TAG"; then
	release_log "tag $RELEASE_TAG already exists locally; refusing to reuse it"
	exit 1
fi

if tag_exists_remote "$REMOTE" "$RELEASE_TAG"; then
	release_log "tag $RELEASE_TAG already exists on $REMOTE; refusing to overwrite or reuse it"
	exit 1
fi

release_status="$(fetch_release_status "$GITHUB_API_URL" "$GITHUB_REPOSITORY" "$RELEASE_TAG" "$GITHUB_TOKEN")"
if ! release_status_ok "$release_status"; then
	if [[ "$release_status" == "200" ]]; then
		release_log "a GitHub release for $RELEASE_TAG already exists; refusing to overwrite it"
	else
		release_log "could not verify release uniqueness for $RELEASE_TAG (GitHub API returned $release_status)"
	fi
	exit 1
fi

release_log "commit $TARGET_SHA verified as the releasable tip of $MAIN_REF; tag/release $RELEASE_TAG is free to create"
