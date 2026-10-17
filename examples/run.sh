#!/usr/bin/env bash
# Runs examples and fails as soon as one fails. Every example checks its own
# results and exits non-zero when one is wrong, so a passing run means the
# examples behaved as documented, not just that they didn't crash.
#
# Usage: examples/run.sh [example ...]   e.g. examples/run.sh 01-hello-natsql
#        (no arguments: every example)
set -euo pipefail
cd "$(dirname "$0")"

run_example() {
	local name=$1
	echo "━━━ Running $name"
	case "$name" in
	06-cli-server) ./06-cli-server/smoke.sh ;;                        # the natsql binary plus a client
	07-perf-benchmark) (cd "$name" && go run . -events=2000) ;;       # a benchmark: small dataset
	*) (cd "$name" && go run .) ;;
	esac
}

if [ $# -eq 0 ]; then
	set -- [0-9][0-9]-*/
fi
for example in "$@"; do
	example=${example%/}
	if [ ! -d "$example" ]; then
		echo "unknown example: $example" >&2
		exit 1
	fi
	run_example "$example"
done
