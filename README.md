# Distributed Inventory Management System

A Python inventory application with a FastAPI dashboard, gRPC services, SQLite storage, and a three-node Raft cluster. The dashboard supports login, ordering, warehouse views, supplier restocking, delivery tracking, forecasts, and optional Ollama insights.

## How the cluster works

1. The web client asks the three Raft nodes for their status. It selects a leader in the highest observed term when at least two distinct nodes agree, then connects to that leader’s advertised inventory address. During an election the web client can return HTTP 503.
2. The application validates the order and calls its local Raft service using `SubmitOrder`. The leader records the command and sends log entries to peers using `AppendEntries`. A majority of this three-node cluster is two nodes.
3. Nodes apply committed order entries to their own SQLite databases. The order carries a user ID, product ID, quantity, and idempotency key. A retry of the same purchase should reuse the same key.
4. If the leader stops, the two remaining nodes can elect a replacement. Election terms and votes survive restart. Voting checks the candidate’s latest log term and index. A restarted follower can catch up from the leader.

| Files | Responsibility |
| --- | --- |
| `web_client.py`, `leader_discovery.py` | Browser routes and discovery of the leader’s inventory service |
| `app_server.py`, `database.py` | Authentication, inventory API, warehouse workflows, analytics, and SQLite schema |
| `raft_node.py`, `raft_server.py` | Elections, timers, peer communication, and Raft RPC handlers |
| `raft_replication.py` | Durable log, follower replication, commit tracking, and application of order entries |
| `inventory.proto`, `raft.proto` | Application, LLM, and Raft message definitions |
| `llm_server.py` | Optional model-generated inventory insights |

## Current integration scope

Every inventory write is a Raft command. The application server sends it to
its local Raft service with `SubmitCommand`; the leader appends it to the
log, replicates it, and once a majority has it every node applies it with the
same `database.apply_command` logic inside the log-application transaction.
Business rejections (not enough stock in the chosen warehouse, an invalid
delivery transition) are recorded as results, so all nodes reach the same
decision.

| Operation | Raft command | Behavior on every node |
| --- | --- | --- |
| Product orders | `PLACE_ORDER` | Checks total stock and the chosen warehouse (or allocates automatically), updates per-warehouse and total stock, records the order, its allocations and a `PLACED` delivery event |
| Supplier restocking | `CREATE_RESTOCK`, `UPDATE_RESTOCK` | Creates the request; moves it through approval and shipping; adds stock to the warehouse on `RECEIVED` |
| Delivery tracking | `UPDATE_DELIVERY` | Advances the delivery status; cancelling before shipping returns units to their original warehouses |
| Login and reads | not replicated | Sessions and reads use the selected node's local database. A user may need to sign in again after failover. Strongly consistent reads would still need a leader-side majority check. |

Each command carries the leader's timestamp and the submitting user's
idempotency key, so a retry is applied at most once and the resulting rows
match across nodes. `tests/test_raft_warehouse_integration.py` drives a
simulated three-node cluster through all four commands and checks that every
node ends with identical stock, allocations and histories.

## Start with Docker Compose

Run from the repository root with Docker Desktop or Docker Engine and the Compose plugin:

```bash
docker compose up -d --build
docker compose exec web python scripts/demo.py wait --timeout 60
docker compose exec web python scripts/demo.py status
```

Open [the dashboard](http://localhost:8000). Demo users are `alice` and `bob`, both with password `password`. Each node container runs its Raft and application services together. Its own named volume holds its database and election state. On first start the entrypoint seeds demo users, products, and warehouses.

| Service | Address |
| --- | --- |
| Browser dashboard | `localhost:8000` |
| Raft from the host | `localhost:50061`, `localhost:50062`, `localhost:50063` |
| Raft inside Compose | `node1:50061`, `node2:50061`, `node3:50061` |
| Inventory inside Compose | `node1:50052`, `node2:50052`, `node3:50052` |
| LLM inside Compose | `llm:50051` |

Run `docker compose down` to stop the cluster and keep its volumes. Adding `--volumes` deletes the saved databases, logs, and election state. The scripts in `scripts/` also provide start, demo, and stop commands for Bash and PowerShell.

## Demonstrate failover and recovery

Use a fresh demo cluster for the example counts below. The wireless mouse (`item_102`) starts with 20 units.

1. Check status, sign in, and order two mice. Record the leader ID and verify the order succeeded and aggregate stock is 18.
2. Stop the leader container. For example, if node 2 won, use `docker compose stop node2`. Run the status command again after several seconds. The surviving nodes should agree on a leader in a higher term.
3. Sign in again if needed. Verify aggregate stock is still 18 and the order appears once. An RPC-level retry with the original idempotency key should leave both counts unchanged.
4. While the old leader is down, order one more mouse with a new key. Restart the stopped container using `docker compose start node2` (substitute its actual name). After it catches up, inspect **that node’s own database**. It should show 17 mice and two orders.

For example, inspect aggregate stock on a recovered node 2:

```bash
docker compose exec node2 python -c "import sqlite3; db=sqlite3.connect('file:/data/inventory.db?mode=ro', uri=True); print(db.execute(\"SELECT stock FROM products WHERE item_id='item_102'\").fetchone()); print(db.execute(\"SELECT COUNT(*) FROM orders WHERE item_id='item_102'\").fetchone())"
```

## Run locally without Docker

Use Python 3.10 or newer and install dependencies from the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Give each node a separate database. Its **Raft and application processes must use the same database path**. From the same project folder, initialize each demo database once:

```bash
for n in 1 2 3; do INVENTORY_DB_PATH="data/node$n.db" python seed_db.py --if-empty; done
```

Use the following pairing. Run each process in its own activated terminal.

| Node | Raft port | Other Raft ports | Inventory port | Database |
| --- | --- | --- | --- | --- |
| 1 | 50061 | 50062, 50063 | 50171 | data/node1.db |
| 2 | 50062 | 50061, 50063 | 50172 | data/node2.db |
| 3 | 50063 | 50061, 50062 | 50173 | data/node3.db |

Example for node 1, in two separate terminals:

```bash
INVENTORY_DB_PATH=data/node1.db python raft_server.py --node-id 1 --address localhost:50061 --peers localhost:50062 localhost:50063 --app-address localhost:50171
```

```bash
INVENTORY_DB_PATH=data/node1.db RAFT_SERVER_ADDRESS=localhost:50061 APP_SERVER_ADDRESS=localhost:50171 python app_server.py
```

Repeat for nodes 2 and 3 using their table rows. Run `python web_client.py` in another terminal. Use `python test_raft_failover.py` for a status snapshot or `python leader_discovery.py` to print the discovered inventory address. Bare `python seed_db.py` resets demo business data. Avoid running it against an existing cluster with a saved Raft log.

## Forecasts and local AI

The dashboard calculates a seven-day demand forecast from sales history and suggests reordering when expected demand exceeds stock. Click **Run AI Analytics** for the report. Calculated results remain available when model insights time out. For local AI, run Ollama with an installed model (default `qwen3:8b`) and start `python llm_server.py` when running outside Docker. Use `OLLAMA_MODEL` to choose a model and `USE_LLM_FORECASTS=true` to enable model-generated forecasts.

Compose defaults to Ollama on the host at `http://host.docker.internal:11434`. To use the optional model container:

```bash
OLLAMA_HOST=http://ollama:11434 docker compose --profile ollama up -d --build
docker compose exec ollama ollama pull qwen3:8b
```

## Development checks

Run the warehouse and replication tests against temporary databases (no
services need to be running):

```bash
python -m unittest tests/test_warehouse_features.py tests/test_warehouse_rpc.py tests/test_raft_warehouse_integration.py
```

The integration suite wires the real Raft servicer, log store and
application server together in-process as a simulated three-node cluster.
Use the separate-node failover procedure above to check the live cluster. For dashboard behavior, run `python -m unittest discover -s tests -p 'test_dashboard_optimization.py'`.

After editing a protocol, regenerate its bindings in the active environment. The integration branch regenerated the Raft bindings with `grpcio-tools` 1.84.0 for a `grpcio` 1.84.0 runtime:

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto
```

Use the same command with `inventory.proto` for application protocol changes.
