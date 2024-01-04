#!/usr/bin/env bash
# Simple benchmark: write N keys then read them back.
#
# Usage: ./bench.sh [server-addr] [num-keys]

set -euo pipefail

SERVER="${1:-localhost:50051}"
NUM_KEYS="${2:-1000}"
CLIENT="go run ../../cmd/client/main.go"

# If the binary exists, use it directly.
if command -v raft-kv-client &> /dev/null; then
    CLIENT="raft-kv-client"
fi

echo "Benchmark: $NUM_KEYS keys against $SERVER"
echo ""

# Write phase
echo "--- PUT phase ---"
START=$(date +%s%N)
for i in $(seq 1 "$NUM_KEYS"); do
    $CLIENT --server "$SERVER" put "bench-key-$i" "value-$i" > /dev/null 2>&1
done
END=$(date +%s%N)
PUT_MS=$(( (END - START) / 1000000 ))
echo "PUT $NUM_KEYS keys in ${PUT_MS}ms ($(( NUM_KEYS * 1000 / (PUT_MS + 1) )) ops/sec)"

# Read phase
echo ""
echo "--- GET phase ---"
START=$(date +%s%N)
for i in $(seq 1 "$NUM_KEYS"); do
    $CLIENT --server "$SERVER" get "bench-key-$i" > /dev/null 2>&1
done
END=$(date +%s%N)
GET_MS=$(( (END - START) / 1000000 ))
echo "GET $NUM_KEYS keys in ${GET_MS}ms ($(( NUM_KEYS * 1000 / (GET_MS + 1) )) ops/sec)"

echo ""
echo "Done."
