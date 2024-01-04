#!/usr/bin/env bash
# Simulate network partitions using docker network disconnect/connect.
#
# Usage:
#   ./partition.sh isolate <container>   # Disconnect a node from the cluster network
#   ./partition.sh heal <container>      # Reconnect a node to the cluster network

set -euo pipefail

NETWORK="raft_kv_raft-net"

usage() {
    echo "Usage: $0 {isolate|heal} <container-name>"
    echo ""
    echo "Examples:"
    echo "  $0 isolate raft-node1    # Partition node1 from the cluster"
    echo "  $0 heal raft-node1       # Reconnect node1 to the cluster"
    exit 1
}

if [ $# -ne 2 ]; then
    usage
fi

ACTION="$1"
CONTAINER="$2"

case "$ACTION" in
    isolate)
        echo "Isolating $CONTAINER from network $NETWORK..."
        docker network disconnect "$NETWORK" "$CONTAINER"
        echo "Done. $CONTAINER is now partitioned."
        ;;
    heal)
        echo "Reconnecting $CONTAINER to network $NETWORK..."
        docker network connect "$NETWORK" "$CONTAINER"
        echo "Done. $CONTAINER is back in the cluster."
        ;;
    *)
        usage
        ;;
esac
