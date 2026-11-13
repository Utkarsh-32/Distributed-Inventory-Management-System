#!/usr/bin/env bash
#
# Starts one cluster node: its Raft server and its application server.
#
# Both processes share this container, so stopping the container is a
# realistic "node crashed" event: the Raft node and its database go away
# together, and the remaining nodes elect a new leader.
#
# Each node keeps all of its state under DATA_DIR, which docker-compose.yml
# mounts from a separate named volume per node:
#
#   $DATA_DIR/inventory.db              this node's SQLite database
#   $DATA_DIR/data/node_<id>_state.json this node's Raft term and vote
#
# Required environment:
#   NODE_ID                 Raft node id (1, 2, 3)
#   RAFT_PEERS              space-separated peer Raft addresses
#   APP_ADVERTISE_ADDRESS   address other containers use for this app server
#
# Optional environment:
#   DATA_DIR                default /data
#   RAFT_PORT               default 50061
#   APP_SERVER_ADDRESS      app server listen address, default 0.0.0.0:50052

set -euo pipefail

: "${NODE_ID:?NODE_ID is required}"
: "${RAFT_PEERS:?RAFT_PEERS is required}"
: "${APP_ADVERTISE_ADDRESS:?APP_ADVERTISE_ADDRESS is required}"

DATA_DIR="${DATA_DIR:-/data}"
RAFT_PORT="${RAFT_PORT:-50061}"

export APP_SERVER_ADDRESS="${APP_SERVER_ADDRESS:-0.0.0.0:50052}"
export INVENTORY_DB_PATH="${DATA_DIR}/inventory.db"

mkdir -p "${DATA_DIR}"

echo "[node ${NODE_ID}] data directory: ${DATA_DIR}"

# Seed demo data the first time this node's volume is used.  Restarts keep
# the existing database.
python /app/seed_db.py --if-empty

python /app/app_server.py &
APP_PID=$!

# raft_node.py stores its state in ./data, so run it from the node's data
# directory to keep that state on the node's volume.
(
    cd "${DATA_DIR}"
    # shellcheck disable=SC2086  # RAFT_PEERS is intentionally split
    exec python /app/raft_server.py \
        --node-id "${NODE_ID}" \
        --address "0.0.0.0:${RAFT_PORT}" \
        --peers ${RAFT_PEERS} \
        --app-address "${APP_ADVERTISE_ADDRESS}"
) &
RAFT_PID=$!

shutdown() {
    echo "[node ${NODE_ID}] stopping"
    kill -TERM "${APP_PID}" "${RAFT_PID}" 2>/dev/null || true
}

trap shutdown TERM INT

# If either process exits, stop the other so Docker sees the node as down
# (and restarts it, per the compose restart policy).
set +e
wait -n "${APP_PID}" "${RAFT_PID}"
STATUS=$?
set -e

shutdown
wait || true

exit "${STATUS}"
