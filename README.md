# Distributed Inventory Management System

A Python inventory application with a FastAPI dashboard, gRPC services, SQLite storage, and a three-node Raft cluster. The dashboard supports login, ordering, warehouse views, supplier restocking, delivery tracking, forecasts, and optional Ollama insights.

## How the cluster works

1. The web client asks the three Raft nodes for their status. It selects a leader in the highest observed term when at least two distinct nodes agree, then connects to that leader’s advertised inventory address. During an election the web client can return HTTP 503.
2. The application validates the order and calls its local Raft service using `SubmitOrder`. The leader records the command and sends log entries to peers using `AppendEntries`. A majority of this three-node cluster is two nodes.
3. Orders, transfers, and supplier restock commands use the replicated log. Nodes apply committed order entries to their own SQLite databases. The order carries a user ID, product ID, quantity, and idempotency key. A retry of the same purchase should reuse the same key.
4. If the leader stops, the two remaining nodes can elect a replacement. Election terms and votes survive restart. Voting checks the candidate’s latest log term and index. A restarted follower can catch up from the leader.

| Files | Responsibility |
| --- | --- |
| `web_client.py`, `leader_discovery.py` | Browser routes and discovery of the leader’s inventory service |
| `app_server.py`, `database.py` | Authentication, inventory API, warehouse workflows, analytics, and SQLite schema |
| `raft_node.py`, `raft_server.py` | Elections, timers, peer communication, and Raft RPC handlers |
| `raft_replication.py` | Durable log, follower replication, commit tracking, and transactional application of orders, transfers, and restock entries |
| `inventory.proto`, `raft.proto` | Application, LLM, and Raft message definitions |
| `llm_server.py` | Separate gRPC service for optional model-generated inventory insights |
| `partition_demo.py`, `static/partition-demo.js` | Isolated three-node network-partition experiment |

## Current integration scope

| Operation | Current behavior |
| --- | --- |
| Product orders | The application submits orders through Raft. Applying an order checks total product stock, updates that total, and records the order. |
| Warehouse selection and allocation | A selected warehouse must cover the full order. Auto uses one warehouse when possible and otherwise splits the order in proportion to stock, recording each allocation through Raft. |
| Warehouse transfers | Replicated commands debit the source and credit the destination in one application transaction; total product stock is unchanged. |
| Supplier restocking | Request creation and status changes pass through `SubmitCommand` and the replicated log. Only receipt adds stock. A sale or outgoing transfer leaving a warehouse at 5 units or fewer requests enough to reach 10; an existing pending request prevents duplicate automatic requests. |
| Checkout shortage | The themed confirmation dialog requests missing supply, accounting for incoming stock. Confirmation is revalidated on the server. It does not reserve available stock or automatically complete the customer order. |
| Delivery tracking | The local workflow supports packing, shipping, delivery, and cancellation before shipping. These updates are not replicated. Cancellation returns stock only in the selected node’s database, so it can cause divergence. Do not use delivery cancellation in a replicated-state/failover demonstration. |
| Login and reads | Sessions and inventory reads use the selected node’s local database. A user may need to sign in again after failover. Discovery helps route requests. Strongly consistent reads still require a server-side check that the leader has majority support and has applied the required entries. |

Every replica stores **all three warehouses**; warehouses are logical stock locations, not individual Raft nodes. AppendEntries carries both replication traffic and regular heartbeats. The implementation and focused tests do not establish complete production fault tolerance or linearizability. Membership is fixed at three nodes in this setup.

## Start with Docker Compose

Run from the repository root with Docker Desktop or Docker Engine and the Compose plugin:

```bash
docker compose build node1
docker compose up -d --no-build
docker compose exec web python scripts/demo.py wait --timeout 60
docker compose exec web python scripts/demo.py status
```

All application services share one image, so building `node1` once avoids concurrent builds exporting the same image.

Open [the dashboard](http://localhost:8000). Demo users are `alice` and `bob`, both with password `password`. Each node container runs its Raft and application services together. Its own named volume holds its database and election state. On first start, `seed_db.py --if-empty` seeds demo users, products and warehouses. Restarts retain existing data. Fresh stock includes Laptop 50 (25/15/10 across Bengaluru/Mumbai/Delhi), Wireless Mouse 20 (10/6/4), and Keyboard 35. Ordering 30 laptops with Auto allocates 15/9/6 and leaves 10/6/4.

| Service | Address |
| --- | --- |
| Browser dashboard | `localhost:8000` |
| Raft from the host | `localhost:50061`, `localhost:50062`, `localhost:50063` |
| Raft inside Compose | `node1:50061`, `node2:50061`, `node3:50061` |
| Inventory inside Compose | `node1:50052`, `node2:50052`, `node3:50052` |
| LLM inside Compose | `llm:50051` |

Run `docker compose stop` to stop without removing containers, or `docker compose down` to stop the cluster and keep its volumes. Adding `--volumes` deletes the saved databases, logs, and election state. The scripts in `scripts/` also provide start, demo, and stop commands for Bash and PowerShell. `start.sh --fresh` and `start.ps1 -Fresh` are destructive resets; do not use them on data you need to keep. Never run bare `seed_db.py` over an existing Raft log. For a separate demo dataset, stop the current cluster to free its ports, then use a different Compose project name (`docker compose -p dims-rehearsal ...`) consistently for every command; this creates separate volumes without deleting the originals.

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

Use Python 3.10 or newer (the Docker image uses Python 3.12) and install dependencies from the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Run activation only after creating the environment, without a trailing backslash. You can also call `.venv/bin/python` directly. Stop Docker nodes first if using the same host ports.

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

The dashboard calculates a seven-day demand forecast from sales history and suggests reordering when expected demand exceeds stock. Click **Run AI Analytics** for the report. By default `USE_LLM_FORECASTS=false`: numerical forecasts use weighted recent and 30-day sales averages, while the model contributes qualitative insights. With this default, calculated results remain available when optional model insights time out; a fallback is not evidence of successful model inference. The LLM server is reached over gRPC and calls Ollama over HTTP. For local AI, run Ollama with an installed model (default `qwen3:8b`) and start `python llm_server.py` when running outside Docker. Use `OLLAMA_MODEL` to choose a model and `USE_LLM_FORECASTS=true` to enable model-generated forecasts.

Compose defaults to Ollama on the host at `http://host.docker.internal:11434`. To use the optional model container:

```bash
docker compose build node1
OLLAMA_HOST=http://ollama:11434 docker compose --profile ollama up -d --no-build
docker compose exec ollama ollama pull qwen3:8b
```

## Partition experiment

In the dashboard, select **Partition demo**, then **Isolate current leader**. This starts three temporary Raft processes and databases with one demo laptop; it does not partition or change the main warehouse cluster. An RPC interceptor blocks Raft communication across the chosen partition. The minority purchase remains uncommitted while the majority commits one sale. Click **Reconnect cluster** and wait for the verified result: one successful purchase, zero stock and matching logs. Links restore automatically after approximately two minutes.

## Development checks

Use the explicit isolated suite from the repository root:

```bash
python scripts/run_tests.py
```

It selects the warehouse, automatic-restock, transfer, sales-history, analytics, cluster-monitor and application-handler tests, plus the real three-process partition test, vote/persistence regressions and an in-process election test with mocked transport. Temporary databases and election-state directories are used. The partition test needs permission to bind local sockets. Database and mocked-handler tests alone do not prove distributed consistency; use the failover/recovery procedure above and compare each replica's own database.

The root `test_raft_client.py` and `test_raft_failover.py` are live status probes, not automated failover assertions. The live scripts below **place orders** and require a disposable, running cluster:

```bash
# Inside the web container: its environment supplies internal Raft addresses.
docker compose exec web python scripts/demo.py features
```

The features tour uses replicated workflows only. The separate `tests/test_idempotency.py` is an opt-in live check with `--address` for the current leader's application service. `test_concurrency.py` requires fresh Wireless Mouse stock of 20; never follow an old instruction to reseed an existing cluster. On a failed or partially completed live run, retain the evidence and use a new dataset for repetition.

After editing a protocol, regenerate both bindings in the active environment:

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. inventory.proto raft.proto
```

Dependencies are currently unpinned. Generated bindings include runtime version checks, so keep `grpcio`, `grpcio-tools` and `protobuf` compatible or regenerate using the installed toolchain.

## Troubleshooting

- **No leader / HTTP 503:** ensure at least two nodes are running, wait for election, then inspect `docker compose logs --tail 60 node1 node2 node3`.
- **Login lost after failover:** sign in again; sessions are node-local.
- **Follower behind after restart:** stop new writes, wait and inspect log/commit/applied progress. Matching terms alone do not establish matching inventory.
- **Ollama unavailable:** verify the configured host and installed model; the default numerical forecast still works without qualitative insights.
- **Docker registry certificate names a campus domain:** resolve captive-portal/network access or use an already built compatible image with `up -d --no-build`. Do not disable certificate verification.
- **Port occupied:** identify the owning process/container before stopping it; do not start local nodes on ports already published by Compose.

## Submission notes

The AOS video should cover both milestones within the assignment's 5–10 minute limit. Prioritize replicated state, retries, concurrency and failure/recovery evidence. `REFLECTION.md` contains proposed contributions that each team member must verify; documentation cannot establish who performed the work.
