#!/usr/bin/env bash
# Build and start the whole cluster in Docker, then wait for a leader.
#
#   ./scripts/start.sh            start (keeps existing node data)
#   ./scripts/start.sh --fresh    wipe all node data first
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "${1:-}" == "--fresh" ]]; then
    echo "Removing existing containers and node data..."
    docker compose down --volumes --remove-orphans
fi

docker compose up -d --build

echo
echo "Waiting for the Raft nodes to elect a leader..."
docker compose exec -T web python scripts/demo.py wait --timeout 60

echo
docker compose exec -T web python scripts/demo.py status

echo
echo "Dashboard: http://localhost:8000   (alice / password)"
echo "Logs:      docker compose logs -f"
echo "Demo:      ./scripts/demo.sh"
echo "Stop:      ./scripts/stop.sh"
