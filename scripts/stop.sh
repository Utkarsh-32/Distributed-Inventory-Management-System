#!/usr/bin/env bash
# Stop the cluster.
#
#   ./scripts/stop.sh           stop, keeping each node's data
#   ./scripts/stop.sh --wipe    stop and delete all node data
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "${1:-}" == "--wipe" ]]; then
    docker compose down --volumes --remove-orphans
    echo "Stopped and removed all node data."
else
    docker compose down --remove-orphans
    echo "Stopped. Node data is kept; use --wipe to delete it."
fi
