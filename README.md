# Distributed Inventory Management System

An academic distributed-systems project for managing inventory, orders, and
sales history. It uses Python and gRPC for service-to-service communication,
FastAPI for the browser interface, SQLite for persistent data, and Ollama for
optional local AI insights.

## What it does

- Authenticates users and keeps their sessions in SQLite
- Displays current inventory and each user's recent order history
- Accepts orders atomically and prevents stock from becoming negative
- Uses historical sales data to produce a 7-day demand forecast
- Calculates reorder decisions and quantities from forecast demand and stock
- Shows inventory observations, management actions, and optional local-AI
  insights in the dashboard
- Tracks stock separately in each warehouse and lets orders ship from a
  chosen warehouse or be split automatically across warehouses
- Sends supplier restock requests through an approval and shipping workflow;
  stock is added only when the shipment is received
- Tracks delivery status for every order, with a full update history and
  cancellation that returns stock to the original warehouses
- Runs as a Docker cluster: three nodes (Raft + application server) with
  separate persistent storage, plus the LLM server and web client

## Architecture

```text
Browser
  |
  | HTTP
  v
Web Client (FastAPI) :8000
  |
  | gRPC
  v
Application Server :50052 ---- SQLite: inventory.db
  |
  | gRPC
  v
LLM Server :50051 ---- Ollama: qwen3:8b
```

| Component | Responsibility |
| --- | --- |
| `web_client.py` | Login, dashboard pages, cookies, and browser API endpoints |
| `app_server.py` | Authentication, orders, inventory rules, forecasts, and analytics |
| `llm_server.py` | Structured local-model requests through Ollama |
| `database.py` | SQLite schema, sessions, product data, and transactional orders |
| `inventory.proto` | gRPC service and message definitions |

All communication between the project services uses gRPC. The application
server owns the numeric calculations and inventory rules; the local model
cannot change stock, reorder quantities, or calculated forecasts.

## Multi-warehouse operations

### Warehouse-wise stock

The demo data has three warehouses: **Bengaluru Central**, **Mumbai West**
and **Delhi North**. Each product's stock is split across them, and
`products.stock` always equals the sum of its warehouse stock.

When placing an order, pick a warehouse or leave it on **Auto**:

- **Chosen warehouse**: the whole order ships from it, and it fails if
  that warehouse does not have enough stock.
- **Auto**: the order ships from the warehouses with the most stock first,
  splitting across warehouses only when necessary.

Every order records which warehouses fulfilled it. A warehouse holding five
units of a product or fewer is shown as low stock.

### Supplier restock requests

Request more stock for one product at one warehouse from a supplier. Each
supplier has a lead time used to show the expected arrival date.

```text
REQUESTED -> APPROVED -> SHIPPED -> RECEIVED
    |            |
    +-> REJECTED +-> CANCELLED
    +-> CANCELLED
```

Stock is added to the warehouse, in the same transaction as the status
change, only when a request becomes `RECEIVED`. Steps cannot be skipped, and
repeating a status changes nothing, so retries never add stock twice.

### Delivery updates

Every order starts as `PLACED` and moves through:

```text
PLACED -> PACKED -> SHIPPED -> OUT_FOR_DELIVERY -> DELIVERED
   |         |
   +---------+-> CANCELLED
```

An order can be cancelled until it ships. Cancelling returns its units to
the warehouses they came from and removes the sale from the forecast
history. Each update is kept in a timeline shown under **History** on the
dashboard.

### gRPC operations

These use the existing `ClientService.get` and `ClientService.post` calls,
so `inventory.proto` is unchanged. Responses and payloads are JSON.

| Call | Type | Payload / result |
| --- | --- | --- |
| `get` | `warehouse_stock` | Warehouses and per-product stock by warehouse |
| `get` | `suppliers` | Suppliers and lead times |
| `get` | `restock_requests` | Recent restock requests with allowed next statuses |
| `get` | `deliveries` | Recent orders with allocations and delivery timeline |
| `post` | `order` | `item_id`, `quantity`, optional `warehouse_id` |
| `post` | `restock_request` | `item_id`, `warehouse_id`, `supplier_id`, `quantity`, optional `note` |
| `post` | `restock_status` | `restock_id`, `status`, optional `note` |
| `post` | `delivery_status` | `order_id`, `status`, optional `note` |

Every `post` requires an idempotency key, as orders already did.

## Run with Docker

This is the easiest way to run the full cluster. It needs
[Docker Desktop](https://www.docker.com/products/docker-desktop/) (or Docker
Engine with the Compose plugin).

```text
Browser -> web :8000 -> finds the Raft leader -> leader's app server
                                                    |
           node1 / node2 / node3                    v
           (Raft :50061 + app server :50052,     llm :50051 -> Ollama
            each with its own volume)
```

| Service | Runs | Storage |
| --- | --- | --- |
| `node1`, `node2`, `node3` | `raft_server.py` + `app_server.py` | Volumes `node1-data`, `node2-data`, `node3-data` (SQLite database and Raft state) |
| `llm` | `llm_server.py` | - |
| `web` | `web_client.py` on port 8000 | - |

Each node seeds its own database the first time it starts and keeps it
across restarts. Stopping a node container stops its Raft server and its
application server together, which is how the demo simulates a node crash.

Start, demo and stop:

```bash
# macOS / Linux / Git Bash
./scripts/start.sh --fresh   # build, start, wait for a leader
./scripts/demo.sh            # feature walkthrough + leader failover
./scripts/stop.sh            # stop; add --wipe to delete node data
```

```powershell
# Windows PowerShell
.\scripts\start.ps1 -Fresh
.\scripts\demo.ps1
.\scripts\stop.ps1        # add -Wipe to delete node data
```

Then open [http://localhost:8000](http://localhost:8000) and sign in as
`alice` / `password`.

Useful commands:

```bash
docker compose exec web python scripts/demo.py status   # roles, terms, leader
docker compose logs -f node1                            # one node's logs
docker compose stop node2                               # simulate a crash
docker compose start node2                              # bring it back
```

**Ollama:** the `llm` container uses Ollama on your computer at
`http://host.docker.internal:11434` by default. To run Ollama in Docker
instead:

```bash
OLLAMA_HOST=http://ollama:11434 docker compose --profile ollama up -d --build
docker compose exec ollama ollama pull qwen3:8b
```

Without Ollama, everything except the local-AI insights works; the
dashboard shows a fallback message for those.

**Before replication is merged:** each node has its own database, seeded
with the same demo data. Writes go to the current leader only, so after a
failover the new leader shows its own copy of the data and users sign in
again. Raft log replication keeps the nodes' databases in sync once it is
merged.

### Configuration

| Variable | Used by | Default |
| --- | --- | --- |
| `INVENTORY_DB_PATH` | app server, `seed_db.py` | `./inventory.db` |
| `APP_SERVER_ADDRESS` | app server listen address | `localhost:50052` |
| `LLM_SERVER_ADDRESS` | app server, address of the LLM server | `localhost:50051` |
| `LLM_LISTEN_ADDRESS` | LLM server listen address | `localhost:50051` |
| `RAFT_ADDRESSES` | web client, `scripts/demo.py` (comma-separated) | `localhost:50061,localhost:50062,localhost:50063` |
| `WEB_HOST`, `WEB_PORT` | web client | `127.0.0.1`, `8000` |

`python seed_db.py --if-empty` seeds only a database that has no users yet.

## Requirements

- Python 3.10 or newer
- Ollama installed and running locally
- An Ollama model that supports structured JSON output; `qwen3:8b` is the
  default model

Check available local models:

```bash
ollama list
```

## Setup

Create a virtual environment and install the project dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Download the default local model if needed:

```bash
ollama pull qwen3:8b
```

Initialize the demo database:

```bash
python seed_db.py
```

> `seed_db.py` resets the local demo database. It removes existing users,
> sessions, orders, products, and sales history in `inventory.db` before
> inserting the demo data.

## Run the system

Start the following processes in separate terminals. Activate `.venv` in each
terminal first.

```bash
# Terminal 1: LLM service
python llm_server.py
```

```bash
# Terminal 2: inventory and application service
python app_server.py
```

```bash
# Terminal 3: browser-facing FastAPI service
python web_client.py
```

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000).

Demo credentials:

| Username | Password |
| --- | --- |
| `alice` | `password` |
| `bob` | `password` |

## Using the dashboard

1. Sign in.
2. Review the available inventory or place an order.
3. Click **Run AI Analytics**.
4. Review the demand forecasts, reorder recommendations, product analysis,
   calculated observations/actions, and local-AI insights.

The dashboard does not run the AI workflow automatically at login. This keeps
the initial page load fast and lets the user request analytics explicitly.

### Analytics behavior

The default flow is optimized for a local model:

- Forecasts use a weighted combination of recent 7-day and 30-day sales
  averages.
- Reorder rules are deterministic:

  ```text
  reorder = predicted_7_day_demand > current_stock
  reorder_quantity = max(predicted_7_day_demand - current_stock, 0)
  ```

- The model is used for at most two short qualitative observations.
- AI results are cached for five minutes and invalidated after a successful
  order.
- The LLM process is asked to keep the model loaded for ten minutes, reducing
  repeat startup cost.
- If the model fails, is unavailable, or exceeds the 20-second request limit,
  the dashboard still shows calculated forecasts, recommendations, and
  inventory analytics. It displays an informational fallback in the insights
  section instead of failing the page.

### Optional model-generated forecasts

Deterministic forecasts are the default because they are fast and predictable.
To enable the slower model-generated forecast path, start the application
server with:

```bash
USE_LLM_FORECASTS=true python app_server.py
```

### Use a different local model

Set `OLLAMA_MODEL` before starting the LLM server:

```bash
OLLAMA_MODEL=qwen3:1.7b python llm_server.py
```

Use an installed model that supports structured JSON output. A smaller model
may respond faster but can produce less useful qualitative observations.

## gRPC contract

The service definitions live in `inventory.proto`. Generated files are
included in the repository. Regenerate them after changing the protocol:

```bash
python -m grpc_tools.protoc \
  -I. \
  --python_out=. \
  --grpc_python_out=. \
  inventory.proto
```

The main service operations are:

| Service | Operations |
| --- | --- |
| `ClientService` | Login, logout, protected reads, and order requests |
| `LLMService` | Structured demand, reorder, and analytics requests |
| `AppServerService` | Internal business-request entry point |

## Tests

Run the focused dashboard tests:

```bash
python -m unittest tests/test_dashboard_optimization.py
```

Run the warehouse, restock and delivery tests. They use a temporary database
and need no running services:

```bash
python -m unittest tests/test_warehouse_features.py tests/test_warehouse_rpc.py
```

The following integration checks require the relevant services to be running:

```bash
python tests/test_llm_rpc.py
python tests/test_app_llm_demand.py
python tests/test_app_llm_reorder.py
python tests/test_app_llm_analytics.py
python test_rpc.py
python test_concurrency.py
```

## Project layout

```text
.
├── app_server.py              # Core business logic and application gRPC server
├── llm_server.py              # Ollama-backed LLM gRPC server
├── web_client.py              # FastAPI web server
├── database.py                # SQLite access and transactions
├── seed_db.py                 # Demo-data reset and seeding
├── mock_data.py               # Demo products and sales histories
├── inventory.proto            # gRPC contract
├── raft_node.py, raft_server.py, leader_discovery.py  # Raft leader election
├── templates/                 # Login and dashboard pages
├── static/                    # Browser styles
├── Dockerfile                 # One image for every service
├── docker-compose.yml         # 3 nodes + LLM + web, per-node volumes
├── docker/node-entrypoint.sh  # Starts one node (Raft + app server)
├── scripts/                   # start / stop / demo scripts (.sh and .ps1), demo.py
└── tests/                     # Integration, dashboard and warehouse tests
```

## Troubleshooting

| Problem | Resolution |
| --- | --- |
| The login page cannot connect | Start `app_server.py`, then start `web_client.py`. |
| AI analytics does not load | Confirm all three services are running on ports 8000, 50052, and 50051. |
| Local AI insights are unavailable | Run `ollama list`, ensure the selected model exists, then restart `llm_server.py`. Calculated analytics will still be available. |
| Demo login fails | Run `python seed_db.py`, then use `alice` / `password` or `bob` / `password`. |
| Analytics seem stale | A successful order clears the cache; otherwise wait up to five minutes for it to expire. |
| Ollama is too slow | Keep deterministic forecasts enabled, use a smaller installed model through `OLLAMA_MODEL`, or wait for the initial model load to complete. |

## Current scope

This version implements the Milestone 1 foundation: gRPC service boundaries,
authentication, inventory and order business rules, persistent data, and local
LLM integration. Distributed consensus, replication, failure detection, and
multi-node consistency can be added as future work without replacing the
current inventory or AI layers.
