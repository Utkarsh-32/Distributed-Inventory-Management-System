"""
Command-line demo of the cluster and the warehouse features.

Normally run inside the web container by scripts/demo.sh or
scripts/demo.ps1, where the node hostnames resolve:

    docker compose exec web python scripts/demo.py status
    docker compose exec web python scripts/demo.py features
    docker compose exec web python scripts/demo.py leader
    docker compose exec web python scripts/demo.py order

Commands:
    status    show every node's Raft role and term, and the leader
    leader    print only the leader's service name (for scripts), e.g. node2
    features  walk through warehouse stock, orders, supplier restocks and
              delivery updates against the leader
    order     place one order against the current leader (used after
              failover to show the cluster still accepts requests)
    wait      wait until a leader is elected (up to --timeout seconds)
"""

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import grpc  # noqa: E402

import inventory_pb2  # noqa: E402
import inventory_pb2_grpc  # noqa: E402
import raft_pb2  # noqa: E402
import raft_pb2_grpc  # noqa: E402


RAFT_ADDRESSES = [
    address.strip()
    for address in os.getenv(
        "RAFT_ADDRESSES",
        "localhost:50061,localhost:50062,localhost:50063",
    ).split(",")
    if address.strip()
]

USERNAME = "alice"
PASSWORD = "password"


# ============================================================
# OUTPUT HELPERS
# ============================================================

def heading(text):
    print()
    print("=" * 64)
    print(text)
    print("=" * 64)


def step(text):
    print()
    print(f"--> {text}")


# ============================================================
# CLUSTER
# ============================================================

def cluster_status():
    """Return a list of (address, status-or-None) for every node."""
    results = []

    for address in RAFT_ADDRESSES:
        try:
            with grpc.insecure_channel(address) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                status = stub.GetStatus(raft_pb2.StatusRequest(), timeout=1)
                results.append((address, status))

        except grpc.RpcError:
            results.append((address, None))

    return results


def current_leader(statuses=None):
    """Return the leader's StatusResponse, or None if there is none yet."""
    statuses = statuses if statuses is not None else cluster_status()
    reachable = [status for _, status in statuses if status is not None]

    if not reachable:
        return None

    highest_term = max(status.term for status in reachable)

    leaders = [
        status
        for status in reachable
        if status.role == "leader" and status.term == highest_term
    ]

    return leaders[0] if len(leaders) == 1 else None


def print_status():
    statuses = cluster_status()
    leader = current_leader(statuses)

    print(f"{'Node':<8}{'Address':<18}{'Role':<12}{'Term':<6}App server")

    for address, status in statuses:
        if status is None:
            print(f"{'?':<8}{address:<18}{'DOWN':<12}{'-':<6}-")
            continue

        marker = "  <- leader" if leader and status.node_id == leader.node_id else ""
        print(
            f"node{status.node_id:<4}{address:<18}{status.role:<12}"
            f"{status.term:<6}{status.app_address}{marker}"
        )

    if leader is None:
        print("\nNo leader yet (election in progress).")

    return leader


def wait_for_leader(timeout):
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        leader = current_leader()

        if leader is not None and leader.app_address:
            return leader

        time.sleep(1)

    return None


# ============================================================
# APPLICATION SERVER CALLS
# ============================================================

class Client:
    """Talks to the leader's application server."""

    def __init__(self, leader):
        self.leader = leader
        self.channel = grpc.insecure_channel(leader.app_address)
        self.stub = inventory_pb2_grpc.ClientServiceStub(self.channel)
        self.token = None

    def login(self):
        response = self.stub.login(
            inventory_pb2.LoginRequest(username=USERNAME, password=PASSWORD),
            timeout=10,
        )

        if response.status != "SUCCESS":
            raise SystemExit(f"Login failed: {response.status}")

        self.token = response.token
        print(f"Logged in as {USERNAME} on node{self.leader.node_id} ({self.leader.app_address})")

    def get(self, request_type):
        response = self.stub.get(
            inventory_pb2.GetRequest(token=self.token, type=request_type),
            timeout=10,
        )

        if response.status != "SUCCESS":
            raise SystemExit(f"{request_type} failed: {response.status}")

        return json.loads(response.items[0].data)

    def post(self, request_type, data):
        response = self.stub.post(
            inventory_pb2.PostRequest(
                token=self.token,
                type=request_type,
                data=json.dumps(data),
                idempotency_key=str(uuid.uuid4()),
            ),
            timeout=10,
        )

        mark = "OK " if response.status == "SUCCESS" else "ERR"
        print(f"    [{mark}] {response.message}")
        return response


def print_stock(client, item_ids):
    data = client.get("warehouse_stock")
    names = {w["warehouse_id"]: w["name"] for w in data["warehouses"]}
    order = [w["warehouse_id"] for w in data["warehouses"]]

    header = f"    {'Product':<16}{'Total':>7}" + "".join(
        f"{names[wid][:14]:>16}" for wid in order
    )
    print(header)

    for product in data["products"]:
        if product["item_id"] not in item_ids:
            continue

        row = f"    {product['name']:<16}{product['total_stock']:>7}" + "".join(
            f"{product['by_warehouse'].get(wid, 0):>16}" for wid in order
        )
        print(row)


def latest_order_id(client):
    deliveries = client.get("deliveries")
    return deliveries[0]["order_id"] if deliveries else None


# ============================================================
# COMMANDS
# ============================================================

def require_leader(timeout=30):
    leader = wait_for_leader(timeout)

    if leader is None:
        raise SystemExit("No leader was elected. Is the cluster running?")

    return leader


def run_features():
    heading("1. Cluster status")
    leader = require_leader()
    print_status()

    client = Client(leader)
    client.login()

    heading("2. Warehouse-wise stock")
    data = client.get("warehouse_stock")

    for warehouse in data["warehouses"]:
        print(
            f"    {warehouse['name']:<20} {warehouse['location']:<16} "
            f"{warehouse['total_units']:>5} units, "
            f"{warehouse['low_stock_items']} low-stock products"
        )

    step("Laptop and SSD stock by warehouse")
    print_stock(client, {"item_101", "item_108"})

    heading("3. Orders choose a warehouse")
    step("Order 2 laptops from Delhi North")
    client.post("order", {"item_id": "item_101", "quantity": 2, "warehouse_id": "wh_del"})

    step("Order 40 laptops with automatic allocation (splits across warehouses)")
    client.post("order", {"item_id": "item_101", "quantity": 40})
    split_order_id = latest_order_id(client)

    step("Try to order more laptops than are left (no overselling)")
    client.post("order", {"item_id": "item_101", "quantity": 1000})

    print_stock(client, {"item_101"})

    heading("4. Supplier restock request")
    step("Request 20 SSDs for Delhi North from QuickParts Express")
    client.post(
        "restock_request",
        {
            "item_id": "item_108",
            "warehouse_id": "wh_del",
            "supplier_id": "sup_quickparts",
            "quantity": 20,
            "note": "Demo restock",
        },
    )
    restock_id = client.get("restock_requests")[0]["id"]

    step("Receiving before approval is rejected")
    client.post("restock_status", {"restock_id": restock_id, "status": "RECEIVED"})

    for status in ["APPROVED", "SHIPPED", "RECEIVED"]:
        step(f"Mark restock #{restock_id} {status}")
        client.post("restock_status", {"restock_id": restock_id, "status": status})

    step("SSD stock after the shipment arrives")
    print_stock(client, {"item_108"})

    heading("5. Delivery updates")
    step("Order 1 SSD and track its delivery")
    client.post("order", {"item_id": "item_108", "quantity": 1})
    order_id = latest_order_id(client)

    for status in ["PACKED", "SHIPPED", "OUT_FOR_DELIVERY", "DELIVERED"]:
        client.post("delivery_status", {"order_id": order_id, "status": status})

    step(f"Cancel the 40-laptop order #{split_order_id}; stock returns to its warehouses")
    client.post("delivery_status", {"order_id": split_order_id, "status": "CANCELLED"})
    print_stock(client, {"item_101"})

    heading("Done")
    print("Open http://localhost:8000 (alice / password) to see the same data in the dashboard.")


def run_single_order():
    leader = require_leader(timeout=60)
    print_status()

    client = Client(leader)
    client.login()

    step("Place an order on the new leader")
    client.post("order", {"item_id": "item_103", "quantity": 1})


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["status", "leader", "features", "order", "wait"])
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()

    if args.command == "status":
        print_status()

    elif args.command == "leader":
        leader = wait_for_leader(args.timeout)

        if leader is None:
            raise SystemExit(1)

        print(f"node{leader.node_id}")

    elif args.command == "wait":
        leader = wait_for_leader(args.timeout)

        if leader is None:
            raise SystemExit(f"No leader after {args.timeout} seconds.")

        print(f"Leader: node{leader.node_id} ({leader.app_address})")

    elif args.command == "features":
        run_features()

    elif args.command == "order":
        run_single_order()


if __name__ == "__main__":
    main()
