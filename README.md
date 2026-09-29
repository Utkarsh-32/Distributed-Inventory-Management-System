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
├── templates/                 # Login and dashboard pages
├── static/                    # Browser styles
└── tests/                     # Integration and dashboard tests
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
