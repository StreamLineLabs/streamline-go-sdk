#!/usr/bin/env bats

setup() {
	REPO_ROOT="$(cd "$BATS_TEST_DIRNAME/../../.." && pwd)"
	WORKFLOW="$REPO_ROOT/.github/workflows/integration.yml"
}

line_of() {
	grep -n -F "$1" "$WORKFLOW" | head -n1 | cut -d: -f1
}

@test "integration workflow requires a repository-configured image digest" {
	run grep -n 'STREAMLINE_INTEGRATION_IMAGE: \${{ vars.STREAMLINE_INTEGRATION_IMAGE }}' "$WORKFLOW"
	[ "$status" -eq 0 ]
}

@test "integration workflow has no mutable Streamline image tag fallback" {
	run grep -n -E 'streamline:(latest|[0-9]+\.[0-9]+\.[0-9]+)' "$WORKFLOW"
	[ "$status" -ne 0 ]
}

@test "integration workflow verifies the digest before starting the fixture" {
	verify_line="$(line_of 'name: Validate and pull immutable integration image')"
	start_line="$(line_of 'name: Start digest-pinned Streamline fixture')"
	[ -n "$verify_line" ]
	[ -n "$start_line" ]
	[ "$verify_line" -lt "$start_line" ]

	run grep -n 'scripts/release/verify-server-image.sh' "$WORKFLOW"
	[ "$status" -eq 0 ]
}

@test "integration fixture and tests use the immutable image and uncached test execution" {
	run grep -n '"$STREAMLINE_INTEGRATION_IMAGE"' "$WORKFLOW"
	[ "$status" -eq 0 ]
	run grep -n -E 'go test -v -count=1 -tags=integration' "$WORKFLOW"
	[ "$status" -eq 0 ]
}

@test "integration fixture cleanup always runs" {
	cleanup_line="$(line_of 'name: Stop Streamline fixture')"
	[ -n "$cleanup_line" ]
	next_line=$((cleanup_line + 1))
	sed -n "${next_line}p" "$WORKFLOW" | grep -q 'if: always()'
}
