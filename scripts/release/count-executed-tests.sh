#!/usr/bin/env bash
#
# Guards against a conformance run that "passes" only because it never
# actually exercised the server: a wrong -run pattern, a missing
# `-tags=integration`, all tests skipping because the fixture never came up,
# or a crashed/truncated run. `go test` reports a clean exit code (0) in
# several of those cases, so exit-code alone cannot be trusted.
#
# Consumes a `go test -json` stream (default: stdin) and enforces:
#   * at least one test actually executed (reached pass or fail, not skip),
#   * the executed count meets an explicit minimum,
#   * no test reported a "fail" action,
#   * no package result was served from Go's test cache,
#   * the stream contains a terminating package-level result, guarding
#     against truncated/crashed output that never reaches a real summary.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/release/lib.sh
source "$SCRIPT_DIR/lib.sh"

usage() {
	echo "usage: $(basename "$0") --min N [--input FILE]" >&2
	exit 2
}

min=""
input="-"
while [[ $# -gt 0 ]]; do
	case "$1" in
	--min)
		min="${2:-}"
		shift 2
		;;
	--input)
		input="${2:-}"
		shift 2
		;;
	*)
		usage
		;;
	esac
done

if [[ -z "$min" || ! "$min" =~ ^[0-9]+$ ]]; then
	release_log "count-executed-tests: --min must be a non-negative integer, got '$min'"
	usage
fi

if [[ "$input" != "-" && ! -f "$input" ]]; then
	release_log "count-executed-tests: input file not found: '$input'"
	release_log "count-executed-tests: treating a missing go test -json capture as a hard failure"
	exit 1
fi

if [[ "$input" == "-" ]]; then
	data="$(cat)"
else
	data="$(cat "$input")"
fi

if [[ -z "$data" ]]; then
	release_log "count-executed-tests: no go test -json output captured; treating as a hard failure"
	exit 1
fi

executed_count="$(printf '%s\n' "$data" | jq -r '
        select(.Test != null and (.Action == "pass" or .Action == "fail")) | .Test
    ' | sort -u | grep -c '.' || true)"

skipped_count="$(printf '%s\n' "$data" | jq -r '
        select(.Test != null and .Action == "skip") | .Test
    ' | sort -u | grep -c '.' || true)"

failed_names="$(printf '%s\n' "$data" | jq -r '
        select(.Test != null and .Action == "fail") | .Test
    ' | sort -u)"

cached_packages="$(printf '%s\n' "$data" | jq -r '
        select((.Output // "") | contains("(cached)")) |
        (.Package // "<unknown package>")
    ' | sort -u)"

package_terminal="$(printf '%s\n' "$data" | jq -r '
        select(.Test == null and (.Action == "pass" or .Action == "fail")) | .Action
    ' | tail -n1)"

release_log "count-executed-tests: executed=$executed_count skipped=$skipped_count min-required=$min"

if [[ -n "$failed_names" ]]; then
	release_log "count-executed-tests: failing tests detected:"
	while IFS= read -r name; do
		release_log "  - $name"
	done <<<"$failed_names"
	exit 1
fi

if [[ -n "$cached_packages" ]]; then
	release_log "count-executed-tests: cached package output detected; refusing stale conformance evidence:"
	while IFS= read -r package; do
		release_log "  - $package"
	done <<<"$cached_packages"
	exit 1
fi

if [[ -z "$package_terminal" ]]; then
	release_log "count-executed-tests: no terminating package result found in the go test -json output; the run may have crashed or been truncated"
	exit 1
fi

if [[ "$executed_count" -eq 0 && "$min" -gt 0 ]]; then
	release_log "count-executed-tests: zero tests executed (all skipped or none matched); refusing to treat this as a passing conformance run"
	exit 1
fi

if [[ "$executed_count" -lt "$min" ]]; then
	release_log "count-executed-tests: only $executed_count test(s) executed, need at least $min"
	exit 1
fi

release_log "count-executed-tests: OK ($executed_count executed >= $min required)"
