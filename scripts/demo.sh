#!/usr/bin/env bash
# Walk through the project for a demo:
#   1. warehouse stock, orders, replicated supplier restocks
#   2. leader failure: stop the leader node, show a new leader is elected
#      and still accepts orders, then bring the old node back
#
# Start the cluster first with ./scripts/start.sh on disposable demo data.
set -euo pipefail

cd "$(dirname "$0")/.."

pause() {
    if [[ -t 0 ]]; then
        echo
        read -r -p "Press Enter to continue... " _
    fi
}

run() {
    docker compose exec -T web python scripts/demo.py "$@"
}

run features
pause

echo
echo "================================================================"
echo "6. Leader failover"
echo "================================================================"

LEADER=$(run leader --timeout 30)
echo "Current leader: ${LEADER}. Stopping its container (Raft + app server)..."
docker compose stop "${LEADER}"

echo "Waiting for the remaining nodes to elect a new leader..."
run wait --timeout 60
run order
# Committed orders and restocks replicate; compare each node’s own database
# after catch-up before claiming that inventory state matches.

pause

echo
echo "Restarting ${LEADER}; it rejoins the cluster as a follower..."
docker compose start "${LEADER}"
run wait --timeout 60
run status
