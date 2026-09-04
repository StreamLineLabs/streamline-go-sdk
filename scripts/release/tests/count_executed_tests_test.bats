#!/usr/bin/env bats
# Tests for the robust conformance executed-test-count guard
# (scripts/release/count-executed-tests.sh). Uses canned `go test -json`
# style fixtures so the guard's behavior is verified without needing a
# live server, Docker, or a real Go test run.

setup() {
	REPO_ROOT="$(cd "$BATS_TEST_DIRNAME/../../.." && pwd)"
	SCRIPT="$REPO_ROOT/scripts/release/count-executed-tests.sh"
	TMP="$(mktemp -d)"
}

teardown() {
	rm -rf "$TMP"
}

json_line() {
	# $1=Action $2=Test (optional)
	if [[ -n "${2-}" ]]; then
		printf '{"Action":"%s","Test":"%s"}\n' "$1" "$2"
	else
		printf '{"Action":"%s"}\n' "$1"
	fi
}

@test "passes when enough tests executed and a package pass terminates the stream" {
	{
		json_line run TestP01_SimpleProduce
		json_line pass TestP01_SimpleProduce
		json_line run TestP02_KeyedProduce
		json_line pass TestP02_KeyedProduce
		json_line pass
	} >"$TMP/out.json"

	run bash "$SCRIPT" --min 2 --input "$TMP/out.json"
	[ "$status" -eq 0 ]
	[[ "$output" == *"OK (2 executed >= 2 required)"* ]]
}

@test "fails when fewer tests executed than the minimum" {
	{
		json_line run TestP01_SimpleProduce
		json_line pass TestP01_SimpleProduce
		json_line pass
	} >"$TMP/out.json"

	run bash "$SCRIPT" --min 2 --input "$TMP/out.json"
	[ "$status" -ne 0 ]
	[[ "$output" == *"only 1 test(s) executed, need at least 2"* ]]
}

@test "fails hard when every test was skipped (zero executed)" {
	{
		json_line run TestP01_SimpleProduce
		json_line skip TestP01_SimpleProduce
		json_line run TestP02_KeyedProduce
		json_line skip TestP02_KeyedProduce
		json_line pass
	} >"$TMP/out.json"

	run bash "$SCRIPT" --min 2 --input "$TMP/out.json"
	[ "$status" -ne 0 ]
	[[ "$output" == *"zero tests executed"* ]]
}

@test "fails when the go test -json stream is empty (nothing ran at all)" {
	: >"$TMP/out.json"
	run bash "$SCRIPT" --min 2 --input "$TMP/out.json"
	[ "$status" -ne 0 ]
	[[ "$output" == *"no go test -json output captured"* ]]
}

@test "fails when the input file does not exist" {
	run bash "$SCRIPT" --min 2 --input "$TMP/does-not-exist.json"
	[ "$status" -ne 0 ]
	[[ "$output" == *"input file not found"* ]]
}

@test "fails when a test reports a fail action, even if the count meets the minimum" {
	{
		json_line run TestP01_SimpleProduce
		json_line pass TestP01_SimpleProduce
		json_line run TestP02_KeyedProduce
		json_line fail TestP02_KeyedProduce
		json_line fail
	} >"$TMP/out.json"

	run bash "$SCRIPT" --min 1 --input "$TMP/out.json"
	[ "$status" -ne 0 ]
	[[ "$output" == *"failing tests detected"* ]]
	[[ "$output" == *"TestP02_KeyedProduce"* ]]
}

@test "fails when go test reports cached package output" {
	{
		printf '{"Action":"run","Package":"example/conformance","Test":"TestP01_SimpleProduce"}\n'
		printf '{"Action":"pass","Package":"example/conformance","Test":"TestP01_SimpleProduce"}\n'
		printf '{"Action":"output","Package":"example/conformance","Output":"ok  example/conformance  (cached)\\n"}\n'
		printf '{"Action":"pass","Package":"example/conformance"}\n'
	} >"$TMP/out.json"

	run bash "$SCRIPT" --min 1 --input "$TMP/out.json"
	[ "$status" -ne 0 ]
	[[ "$output" == *"cached package output detected"* ]]
	[[ "$output" == *"example/conformance"* ]]
}

@test "fails when the stream is truncated with no terminating package result" {
	{
		json_line run TestP01_SimpleProduce
		json_line pass TestP01_SimpleProduce
		json_line run TestP02_KeyedProduce
		json_line pass TestP02_KeyedProduce
		# No package-level pass/fail action: simulates a crash or truncated log.
	} >"$TMP/out.json"

	run bash "$SCRIPT" --min 2 --input "$TMP/out.json"
	[ "$status" -ne 0 ]
	[[ "$output" == *"no terminating package result"* ]]
}

@test "deduplicates repeated pass events for the same test (retries/subtests still count once)" {
	{
		json_line run TestP01_SimpleProduce
		json_line pass TestP01_SimpleProduce
		json_line pass TestP01_SimpleProduce
		json_line pass
	} >"$TMP/out.json"

	run bash "$SCRIPT" --min 1 --input "$TMP/out.json"
	[ "$status" -eq 0 ]
	[[ "$output" == *"executed=1"* ]]
}

@test "rejects a non-numeric --min value" {
	{
		json_line pass
	} >"$TMP/out.json"
	run bash "$SCRIPT" --min abc --input "$TMP/out.json"
	[ "$status" -ne 0 ]
}

@test "reads from stdin when --input is omitted" {
	{
		json_line run TestP01_SimpleProduce
		json_line pass TestP01_SimpleProduce
		json_line pass
	} >"$TMP/out.json"

	run bash -c "bash '$SCRIPT' --min 1 < '$TMP/out.json'"
	[ "$status" -eq 0 ]
}
