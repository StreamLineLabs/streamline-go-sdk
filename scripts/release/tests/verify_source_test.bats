#!/usr/bin/env bats
# Tests for scripts/release/verify-source.sh using disposable local git
# repositories (acting as both the checkout and the "remote") so commit
# reachability/currency and tag-existence checks run against real git
# plumbing without any network access. The GitHub "release already exists"
# check is exercised through a stubbed `curl` on PATH returning canned
# status codes, since it is the one dependency that would otherwise need a
# live GitHub API call.

setup() {
	REPO_ROOT="$(cd "$BATS_TEST_DIRNAME/../../.." && pwd)"
	SCRIPT="$REPO_ROOT/scripts/release/verify-source.sh"

	REMOTE="$(mktemp -d)"
	WORK="$(mktemp -d)"
	git init -q --bare "$REMOTE"
	git clone -q "$REMOTE" "$WORK"
	git -C "$WORK" -c user.email=test@example.com -c user.name=test commit -q --allow-empty -m init
	git -C "$WORK" push -q origin HEAD:main
	MAIN_SHA="$(git -C "$WORK" rev-parse HEAD)"

	STUBDIR="$(mktemp -d)"
}

teardown() {
	rm -rf "$REMOTE" "$WORK" "$STUBDIR"
}

# stub_curl <http-status> installs a fake `curl` ahead of the real one on
# PATH that always reports the given status code, so the "release already
# exists" check can be tested for every branch without a network call.
stub_curl() {
	cat >"$STUBDIR/curl" <<EOF
#!/usr/bin/env bash
echo -n "$1"
EOF
	chmod +x "$STUBDIR/curl"
}

run_verify() {
	stub_curl "${RELEASE_HTTP_STATUS:-404}"
	(
		cd "$WORK" &&
			PATH="$STUBDIR:$PATH" \
				RELEASE_TAG="${1-}" TARGET_SHA="${2-}" \
				GITHUB_REPOSITORY=streamlinelabs/streamline-go-sdk GITHUB_TOKEN=dummy \
				bash "$SCRIPT"
	)
}

@test "verify-source.sh accepts the current tip of main with a free tag and no existing release" {
	run run_verify "v9.9.9" "$MAIN_SHA"
	[ "$status" -eq 0 ]
	[[ "$output" == *"verified as the releasable tip"* ]]
}

@test "verify-source.sh rejects a commit that is not the current tip of main" {
	git -C "$WORK" -c user.email=test@example.com -c user.name=test commit -q --allow-empty -m second
	git -C "$WORK" push -q origin HEAD:main
	# MAIN_SHA is now stale relative to origin/main.
	run run_verify "v9.9.9" "$MAIN_SHA"
	[ "$status" -ne 0 ]
	[[ "$output" == *"not the current tip of"* ]]
}

@test "verify-source.sh rejects a commit unreachable from main (on a divergent branch)" {
	git -C "$WORK" checkout -q -b feature
	git -C "$WORK" -c user.email=test@example.com -c user.name=test commit -q --allow-empty -m feature-only
	FEATURE_SHA="$(git -C "$WORK" rev-parse HEAD)"
	run run_verify "v9.9.9" "$FEATURE_SHA"
	[ "$status" -ne 0 ]
	[[ "$output" == *"not reachable from"* ]]
}

@test "verify-source.sh refuses to reuse a tag that already exists on the remote" {
	git -C "$WORK" tag v9.9.9 "$MAIN_SHA"
	git -C "$WORK" push -q origin v9.9.9
	# Delete the local tag so this exercises remote-only detection, distinct
	# from the "already exists locally" case covered separately below.
	git -C "$WORK" tag -d v9.9.9
	run run_verify "v9.9.9" "$MAIN_SHA"
	[ "$status" -ne 0 ]
	[[ "$output" == *"already exists on"* ]]
}

@test "verify-source.sh refuses to reuse a tag that already exists locally" {
	git -C "$WORK" tag v9.9.9 "$MAIN_SHA"
	run run_verify "v9.9.9" "$MAIN_SHA"
	[ "$status" -ne 0 ]
	[[ "$output" == *"already exists locally"* ]]
}

@test "verify-source.sh refuses to overwrite an existing GitHub release (HTTP 200)" {
	RELEASE_HTTP_STATUS=200
	run run_verify "v9.9.9" "$MAIN_SHA"
	[ "$status" -ne 0 ]
	[[ "$output" == *"already exists; refusing to overwrite"* ]]
}

@test "verify-source.sh fails closed when release-existence cannot be verified (unexpected HTTP status)" {
	RELEASE_HTTP_STATUS=503
	run run_verify "v9.9.9" "$MAIN_SHA"
	[ "$status" -ne 0 ]
	[[ "$output" == *"could not verify release uniqueness"* ]]
}

@test "verify-source.sh requires RELEASE_TAG and TARGET_SHA to be set" {
	run bash -c "cd '$WORK' && GITHUB_REPOSITORY=x GITHUB_TOKEN=dummy bash '$SCRIPT'"
	[ "$status" -ne 0 ]
}
