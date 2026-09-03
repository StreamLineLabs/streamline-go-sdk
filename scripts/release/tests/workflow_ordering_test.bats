#!/usr/bin/env bats
# Verifies structural properties of .github/workflows/release.yml that
# unit tests on the shell scripts cannot see: the workflow trigger, job
# ordering/dependencies, the protected-environment gate, and that every
# check (build/vet/test/security/conformance/artifacts/SBOM/checksums/
# signing/attestation) appears strictly before tag creation, which itself
# appears strictly before GitHub release creation.
#
# This uses plain grep/line-number comparisons rather than a YAML parser:
# it is intentionally simple, has no extra runtime dependency, and is
# robust as long as step/job names stay unique in the file (enforced by
# the "unique" tests below).

setup() {
	REPO_ROOT="$(cd "$BATS_TEST_DIRNAME/../../.." && pwd)"
	WORKFLOW="$REPO_ROOT/.github/workflows/release.yml"
}

line_of() {
	grep -n -F "$1" "$WORKFLOW" | head -n1 | cut -d: -f1
}

count_of() {
	grep -c -F "$1" "$WORKFLOW"
}

@test "release.yml exists" {
	[ -f "$WORKFLOW" ]
}

@test "release.yml no longer triggers on tag push" {
	run grep -n "tags:" "$WORKFLOW"
	[ "$status" -ne 0 ]
}

@test "release.yml triggers only on workflow_dispatch" {
	run grep -n "^  workflow_dispatch:" "$WORKFLOW"
	[ "$status" -eq 0 ]
	run grep -n "^  push:" "$WORKFLOW"
	[ "$status" -ne 0 ]
}

@test "release.yml declares the three required workflow_dispatch inputs" {
	for input in release_version release_tag server_image_digest; do
		run grep -n "      $input:" "$WORKFLOW"
		[ "$status" -eq 0 ]
	done
}

@test "the tag-mutating job requires a protected environment" {
	promote_job_line="$(line_of 'promote-release:')"
	environment_line="$(line_of '    environment:')"
	env_name_line="$(line_of 'name: release-promotion')"
	[ -n "$promote_job_line" ]
	[ -n "$environment_line" ]
	[ -n "$env_name_line" ]
	[ "$environment_line" -gt "$promote_job_line" ]
	[ "$env_name_line" -gt "$environment_line" ]
}

@test "the tag-mutating job depends on the validation job via needs:" {
	run grep -n "needs: validate-and-verify" "$WORKFLOW"
	[ "$status" -eq 0 ]
}

@test "the validation job does not request contents:write permission" {
	# Extract just the validate-and-verify job block (up to the next
	# top-level 'promote-release:' job) and confirm it never asks for
	# contents: write, since it must not be able to mutate the repo.
	awk '/^  validate-and-verify:/{flag=1} /^  promote-release:/{flag=0} flag' "$WORKFLOW" >"$BATS_TEST_TMPDIR/validate-job.yml"
	run grep -n "contents: write" "$BATS_TEST_TMPDIR/validate-job.yml"
	[ "$status" -ne 0 ]
}

@test "the tag-mutating job requests contents:write permission" {
	awk '/^  promote-release:/{flag=1} flag' "$WORKFLOW" >"$BATS_TEST_TMPDIR/promote-job.yml"
	run grep -n "contents: write" "$BATS_TEST_TMPDIR/promote-job.yml"
	[ "$status" -eq 0 ]
}

@test "input validation runs before the source-commit verification step" {
	a="$(line_of 'name: Validate promotion inputs')"
	b="$(line_of 'name: Verify source commit is the current, reachable, releasable tip of main')"
	[ -n "$a" ]
	[ -n "$b" ]
	[ "$a" -lt "$b" ]
}

@test "build/vet/test/examples run before the digest-pinned live fixture starts" {
	a="$(line_of 'name: Verify modules and examples')"
	b="$(line_of 'name: Start digest-pinned live conformance fixture')"
	[ "$a" -lt "$b" ]
}

@test "security scans run before the digest-pinned live fixture starts" {
	a="$(line_of 'name: Security scan (govulncheck, testcontainers module)')"
	b="$(line_of 'name: Start digest-pinned live conformance fixture')"
	[ "$a" -lt "$b" ]
}

@test "the server image digest is verified before the live fixture starts" {
	a="$(line_of 'name: Verify server image resolves to the requested immutable digest')"
	b="$(line_of 'name: Start digest-pinned live conformance fixture')"
	[ "$a" -lt "$b" ]
}

@test "the live fixture starts before conformance tests run" {
	a="$(line_of 'name: Start digest-pinned live conformance fixture')"
	b="$(line_of 'name: Run required conformance tests against the live fixture')"
	[ "$a" -lt "$b" ]
}

@test "conformance tests run before the executed-test-count guard" {
	a="$(line_of 'name: Run required conformance tests against the live fixture')"
	b="$(line_of 'name: Enforce conformance executed-test-count guard')"
	[ "$a" -lt "$b" ]
}

@test "release conformance explicitly disables the Go test cache" {
	run grep -n -E "go test -json .*\\-count=1 .*\\-tags=integration" "$WORKFLOW"
	[ "$status" -eq 0 ]
}

@test "the executed-test-count guard step is configured to run with if: always()" {
	guard_line="$(line_of 'name: Enforce conformance executed-test-count guard')"
	next_line=$((guard_line + 1))
	sed -n "${next_line}p" "$WORKFLOW" | grep -q "if: always()"
}

@test "the executed-test-count guard runs before artifact generation" {
	a="$(line_of 'name: Enforce conformance executed-test-count guard')"
	b="$(line_of 'name: Create source archive')"
	[ "$a" -lt "$b" ]
}

@test "artifacts, SBOMs, checksums, signing, and attestation all run before upload, in order" {
	archive="$(line_of 'name: Create source archive')"
	sbom="$(line_of 'name: Generate and validate module SBOMs')"
	checksums="$(line_of 'name: Generate checksums')"
	signing="$(line_of 'name: Sign and verify checksum manifest')"
	attest="$(line_of 'name: Attest release artifact provenance')"
	upload="$(line_of 'name: Upload release artifacts')"

	[ "$archive" -lt "$sbom" ]
	[ "$sbom" -lt "$checksums" ]
	[ "$checksums" -lt "$signing" ]
	[ "$signing" -lt "$attest" ]
	[ "$attest" -lt "$upload" ]
}

@test "no tag or release is created anywhere in the validate-and-verify job" {
	awk '/^  validate-and-verify:/{flag=1} /^  promote-release:/{flag=0} flag' "$WORKFLOW" >"$BATS_TEST_TMPDIR/validate-job.yml"
	run grep -n -E "git tag|git push|gh release create" "$BATS_TEST_TMPDIR/validate-job.yml"
	[ "$status" -ne 0 ]
}

@test "the whole validate-and-verify job (every check) precedes tag creation, which precedes release creation" {
	upload="$(line_of 'name: Upload release artifacts')"
	tag_create="$(line_of 'name: Create and push annotated release tag')"
	release_create="$(line_of 'name: Create GitHub release')"

	[ "$upload" -lt "$tag_create" ]
	[ "$tag_create" -lt "$release_create" ]
}

@test "artifact checksums are re-verified before the tag is created" {
	reverify="$(line_of 'name: Re-verify downloaded artifact checksums before publishing')"
	tag_create="$(line_of 'name: Create and push annotated release tag')"
	[ "$reverify" -lt "$tag_create" ]
}

@test "step names referenced by the ordering assertions above are each unique in the file" {
	for name in \
		"name: Validate promotion inputs" \
		"name: Verify source commit is the current, reachable, releasable tip of main" \
		"name: Verify modules and examples" \
		"name: Security scan (govulncheck, testcontainers module)" \
		"name: Verify server image resolves to the requested immutable digest" \
		"name: Start digest-pinned live conformance fixture" \
		"name: Run required conformance tests against the live fixture" \
		"name: Enforce conformance executed-test-count guard" \
		"name: Create source archive" \
		"name: Generate and validate module SBOMs" \
		"name: Generate checksums" \
		"name: Sign and verify checksum manifest" \
		"name: Attest release artifact provenance" \
		"name: Upload release artifacts" \
		"name: Re-verify downloaded artifact checksums before publishing" \
		"name: Create and push annotated release tag" \
		"name: Create GitHub release"; do
		count="$(count_of "$name")"
		[ "$count" -eq 1 ]
	done
}
