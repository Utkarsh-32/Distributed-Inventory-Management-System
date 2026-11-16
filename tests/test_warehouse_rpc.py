"""
Tests the warehouse, restock and delivery operations through the
application server's gRPC handlers (ClientServiceServicer.get / post).

The handlers are called directly, so no servers need to be running.

Run with:

    python -m unittest tests/test_warehouse_rpc.py
"""

import json
import sys
import tempfile
import threading
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    import grpc  # noqa: F401
except ImportError:  # pragma: no cover - lets the test run without grpcio
    fake_grpc = types.ModuleType("grpc")
    fake_grpc.__version__ = "99.0.0"
    fake_grpc.RpcError = type("RpcError", (Exception,), {})
    fake_utilities = types.ModuleType("grpc._utilities")
    fake_utilities.first_version_is_lower = lambda *_: False
    fake_grpc._utilities = fake_utilities
    sys.modules["grpc"] = fake_grpc
    sys.modules["grpc._utilities"] = fake_utilities

import app_server  # noqa: E402
import database  # noqa: E402
import inventory_pb2  # noqa: E402
import seed_db  # noqa: E402


class WarehouseRpcTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(
            database,
            "DB_PATH",
            Path(self.temp_dir.name) / "inventory.db",
        )
        self.db_patch.start()

        with patch("builtins.print"):
            seed_db.seed_database()

        self.service = object.__new__(app_server.ClientServiceServicer)
        self.service.dashboard_lock = threading.Lock()
        self.service.dashboard_cache = {"cached": True}
        self.service.dashboard_cache_created_at = 1.0

        self.token = database.authenticate_user("alice", "password")
        self.key_counter = 0

    def tearDown(self):
        self.db_patch.stop()
        self.temp_dir.cleanup()

    def get(self, request_type, token=None):
        response = self.service.get(
            inventory_pb2.GetRequest(
                token=token or self.token,
                type=request_type,
            ),
            None,
        )
        data = json.loads(response.items[0].data) if response.items else None
        return response.status, data

    def post(self, request_type, data, key=None):
        self.key_counter += 1
        with patch("builtins.print"):
            return self.service.post(
                inventory_pb2.PostRequest(
                    token=self.token,
                    type=request_type,
                    data=json.dumps(data) if not isinstance(data, str) else data,
                    idempotency_key=key or f"rpc-key-{self.key_counter}",
                ),
                None,
            )

    def test_reads_require_login(self):
        status, _ = self.get("warehouse_stock", token="not-a-token")
        self.assertEqual(status, "UNAUTHORIZED")

    def test_read_operations_return_json(self):
        status, stock = self.get("warehouse_stock")
        self.assertEqual(status, "SUCCESS")
        self.assertEqual(len(stock["warehouses"]), 3)
        self.assertEqual(len(stock["products"]), 12)

        status, suppliers = self.get("suppliers")
        self.assertEqual(status, "SUCCESS")
        self.assertEqual(len(suppliers), 3)

        self.assertEqual(self.get("restock_requests"), ("SUCCESS", []))
        self.assertEqual(self.get("deliveries"), ("SUCCESS", []))

    def test_unknown_get_type_gets_a_response(self):
        status, _ = self.get("teleport")
        self.assertEqual(status, "UNKNOWN_OPERATION")

    def test_existing_inventory_format_is_unchanged(self):
        response = self.service.get(
            inventory_pb2.GetRequest(token=self.token, type="inventory"),
            None,
        )
        self.assertEqual(response.items[0].data, "Name: Laptop | Stock: 50")

    def test_order_with_warehouse_then_delivery_updates(self):
        response = self.post(
            "order",
            {"item_id": "item_101", "quantity": 2, "warehouse_id": "wh_mum"},
        )
        self.assertEqual(response.status, "SUCCESS", response.message)
        self.assertIn("Mumbai West (2)", response.message)
        # A stock change clears the cached analytics.
        self.assertIsNone(self.service.dashboard_cache)

        _, deliveries = self.get("deliveries")
        order_id = deliveries[0]["order_id"]
        self.assertEqual(deliveries[0]["next_statuses"], ["PACKED", "CANCELLED"])

        response = self.post("delivery_status", {"order_id": order_id, "status": "PACKED"})
        self.assertEqual(response.status, "SUCCESS", response.message)

        response = self.post("delivery_status", {"order_id": order_id, "status": "DELIVERED"})
        self.assertEqual(response.status, "INVALID_TRANSITION")

    def test_order_without_warehouse_still_works(self):
        response = self.post("order", {"item_id": "item_101", "quantity": 1})
        self.assertEqual(response.status, "SUCCESS", response.message)

    def test_restock_flow(self):
        response = self.post(
            "restock_request",
            {
                "item_id": "item_108",
                "warehouse_id": "wh_del",
                "supplier_id": "sup_techsource",
                "quantity": 10,
                "note": "Low in Delhi",
            },
        )
        self.assertEqual(response.status, "SUCCESS", response.message)

        _, requests = self.get("restock_requests")
        restock_id = requests[0]["id"]
        self.service.dashboard_cache = {"cached": True}

        for status in ["APPROVED", "SHIPPED", "RECEIVED"]:
            response = self.post(
                "restock_status",
                {"restock_id": restock_id, "status": status},
            )
            self.assertEqual(response.status, "SUCCESS", response.message)

        # Receiving stock clears the cached analytics.
        self.assertIsNone(self.service.dashboard_cache)

        _, stock = self.get("warehouse_stock")
        ssd = next(p for p in stock["products"] if p["item_id"] == "item_108")
        self.assertEqual(ssd["total_stock"], 25)
        self.assertEqual(ssd["by_warehouse"]["wh_del"], 13)

    def test_bad_payloads_are_rejected(self):
        self.assertEqual(self.post("restock_request", {"item_id": "x"}).status, "INVALID_DATA")
        self.assertEqual(self.post("delivery_status", {"order_id": "abc", "status": "PACKED"}).status, "INVALID_DATA")
        self.assertEqual(self.post("order", "not json").status, "INVALID_DATA")
        self.assertEqual(self.post("order", "[1, 2]").status, "INVALID_DATA")
        self.assertEqual(self.post("launch_rocket", {}).status, "UNKNOWN_OPERATION")


if __name__ == "__main__":
    unittest.main()
