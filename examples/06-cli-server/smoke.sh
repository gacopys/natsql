#!/usr/bin/env bash
# Builds the natsql binary, starts it with config.yaml, runs ./client against
# it, then stops it with SIGTERM. Fails if the client finds a wrong result or
# the server doesn't shut down cleanly.
#
# Usage: ./smoke.sh   (from any directory)
set -euo pipefail
cd "$(dirname "$0")"

go build -o natsql-server ../../cmd/natsql
rm -rf data # start from an empty store (config.yaml: store_dir ./data/natsql)

./natsql-server server --config=config.yaml >server.log 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true' EXIT

if ! (cd client && go run .); then
	echo "--- server.log ---"
	cat server.log
	exit 1
fi

kill -TERM "$server"
if ! wait "$server"; then
	echo "natsql server did not shut down cleanly"
	echo "--- server.log ---"
	cat server.log
	exit 1
fi
trap - EXIT
echo "✓ natsql server shut down cleanly"
