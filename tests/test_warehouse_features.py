"""
Tests for multi-warehouse stock, supplier restock requests and delivery
tracking.  They only need the standard library: each test runs against a
fresh SQLite database in a temporary directory.

Run with:

    python -m unittest tests/test_warehouse_features.py
"""

import sqlite3
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import database  # noqa: E402
import seed_db  # noqa: E402


class WarehouseTestCase(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "inventory.db"
        self.db_patch = patch.object(database, "DB_PATH", self.db_path)
        self.db_patch.start()

        with patch("builtins.print"):
            seed_db.seed_database()

        self.user_id = self.query_one(
            "SELECT id FROM users WHERE username = 'alice'"
        )[0]
        self.key_counter = 0

    def tearDown(self):
        self.db_patch.stop()
        self.temp_dir.cleanup()

    # ---------------- helpers ----------------

    def query_one(self, sql, params=()):
        conn = sqlite3.connect(self.db_path)
        try:
            return conn.execute(sql, params).fetchone()
        finally:
            conn.close()

    def next_key(self):
        self.key_counter += 1
        return f"test-key-{self.key_counter}"

    def warehouse_stock(self, warehouse_id, item_id):
        return self.query_one(
            "SELECT stock FROM warehouse_stock "
            "WHERE warehouse_id = ? AND item_id = ?",
            (warehouse_id, item_id),
        )[0]

    def total_stock(self, item_id):
        return self.query_one(
            "SELECT stock FROM products WHERE item_id = ?",
            (item_id,),
        )[0]

    def assert_totals_consistent(self):
        mismatches = self.query_one(
            """
            SELECT COUNT(*)
            FROM products
            WHERE stock != (
                SELECT COALESCE(SUM(stock), 0)
                FROM warehouse_stock
                WHERE warehouse_stock.item_id = products.item_id
            )
            """
        )[0]
        self.assertEqual(mismatches, 0, "warehouse stock must add up to total")

    def order(self, item_id, quantity, warehouse_id=None, key=None):
        return database.place_order(
            user_id=self.user_id,
            item_id=item_id,
            quantity=quantity,
            idempotency_key=key or self.next_key(),
            warehouse_id=warehouse_id,
        )


class SeedTests(WarehouseTestCase):
    def test_seed_splits_stock_across_three_warehouses(self):
        # Laptop starts with 50 units: 50% / 30% / 20%.
        self.assertEqual(self.warehouse_stock("wh_blr", "item_101"), 25)
        self.assertEqual(self.warehouse_stock("wh_mum", "item_101"), 15)
        self.assertEqual(self.warehouse_stock("wh_del", "item_101"), 10)
        self.assert_totals_consistent()

    def test_seed_creates_suppliers(self):
        self.assertEqual(len(database.get_suppliers()), 3)

    def test_existing_database_is_migrated(self):
        # Build a database the way the previous version did: products with a
        # total only, and an orders table without delivery_status.
        legacy_path = Path(self.temp_dir.name) / "legacy.db"
        conn = sqlite3.connect(legacy_path)
        conn.executescript(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE products (
                item_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                stock INTEGER NOT NULL CHECK (stock >= 0)
            );
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                item_id TEXT NOT NULL,
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            INSERT INTO products VALUES ('item_x', 'Legacy Thing', 11);
            """
        )
        conn.commit()
        conn.close()

        with patch.object(database, "DB_PATH", legacy_path):
            database.initialize_database()
            # Running it twice must not duplicate anything.
            database.initialize_database()
            data = database.get_warehouse_stock()

        product = data["products"][0]
        self.assertEqual(product["total_stock"], 11)
        self.assertEqual(sum(product["by_warehouse"].values()), 11)
        self.assertEqual(len(product["by_warehouse"]), 3)


class OrderAllocationTests(WarehouseTestCase):
    def test_order_from_chosen_warehouse(self):
        result = self.order("item_101", 4, warehouse_id="wh_del")

        self.assertTrue(result["success"], result)
        self.assertEqual(self.warehouse_stock("wh_del", "item_101"), 6)
        self.assertEqual(self.warehouse_stock("wh_blr", "item_101"), 25)
        self.assertEqual(self.total_stock("item_101"), 46)
        self.assertIn("Delhi North (4)", result["message"])
        self.assert_totals_consistent()

    def test_chosen_warehouse_without_enough_stock_is_rejected(self):
        # Delhi only holds 10 laptops even though 50 exist in total.
        result = self.order("item_101", 11, warehouse_id="wh_del")

        self.assertFalse(result["success"])
        self.assertEqual(result["status"], "FAILED_INSUFFICIENT_STOCK")
        self.assertIn("Laptop", result["message"])
        self.assertIn("Delhi North", result["message"])
        self.assertEqual(self.total_stock("item_101"), 50)
        self.assert_totals_consistent()

    def test_unknown_warehouse_is_rejected(self):
        result = self.order("item_101", 1, warehouse_id="wh_nowhere")

        self.assertFalse(result["success"])
        self.assertEqual(result["status"], "WAREHOUSE_NOT_FOUND")

    def test_automatic_allocation_uses_largest_warehouse_first(self):
        result = self.order("item_101", 20)

        self.assertTrue(result["success"], result)
        self.assertEqual(self.warehouse_stock("wh_blr", "item_101"), 5)
        self.assertEqual(
            result["allocations"],
            [{"warehouse_id": "wh_blr", "warehouse_name": "Bengaluru Central", "quantity": 20}],
        )
        self.assert_totals_consistent()

    def test_automatic_allocation_splits_when_needed(self):
        result = self.order("item_101", 45)

        self.assertTrue(result["success"], result)
        self.assertEqual(
            [part["quantity"] for part in result["allocations"]],
            [25, 15, 5],
        )
        self.assertEqual(self.total_stock("item_101"), 5)
        self.assert_totals_consistent()

    def test_cannot_oversell_total_stock(self):
        result = self.order("item_101", 51)

        self.assertFalse(result["success"])
        self.assertEqual(result["status"], "FAILED_INSUFFICIENT_STOCK")
        self.assertEqual(self.total_stock("item_101"), 50)

    def test_idempotent_retry_does_not_deduct_twice(self):
        first = self.order("item_101", 3, key="same-key")
        second = self.order("item_101", 3, key="same-key")

        self.assertTrue(first["success"])
        self.assertTrue(second["success"])
        self.assertEqual(first["order_id"], second["order_id"])
        self.assertEqual(self.total_stock("item_101"), 47)
        self.assert_totals_consistent()

    def test_concurrent_orders_never_oversell_a_warehouse(self):
        # Delhi has 10 laptops; 20 threads each try to buy 1 from it.
        results = []
        lock = threading.Lock()

        def buy(index):
            result = self.order(
                "item_101", 1, warehouse_id="wh_del", key=f"concurrent-{index}"
            )
            with lock:
                results.append(result["success"])

        threads = [threading.Thread(target=buy, args=(i,)) for i in range(20)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(results.count(True), 10)
        self.assertEqual(self.warehouse_stock("wh_del", "item_101"), 0)
        self.assert_totals_consistent()


class RestockTests(WarehouseTestCase):
    def create_request(self, quantity=12, key=None):
        return database.create_restock_request(
            user_id=self.user_id,
            item_id="item_108",
            warehouse_id="wh_mum",
            supplier_id="sup_quickparts",
            quantity=quantity,
            idempotency_key=key or self.next_key(),
            note="Running low",
        )

    def move(self, restock_id, status):
        return database.update_restock_status(
            restock_id, status, self.user_id
        )

    def test_full_restock_lifecycle_adds_stock_once(self):
        before_wh = self.warehouse_stock("wh_mum", "item_108")
        before_total = self.total_stock("item_108")

        created = self.create_request()
        self.assertTrue(created["success"], created)
        restock_id = created["restock_id"]

        for status in ["APPROVED", "SHIPPED"]:
            self.assertTrue(self.move(restock_id, status)["success"])
            # Nothing arrives until the shipment is received.
            self.assertEqual(self.total_stock("item_108"), before_total)

        received = self.move(restock_id, "RECEIVED")
        self.assertTrue(received["success"], received)
        self.assertTrue(received["stock_changed"])

        # Receiving again is a no-op.
        again = self.move(restock_id, "RECEIVED")
        self.assertTrue(again["success"])
        self.assertIn("already", again["message"])

        self.assertEqual(self.warehouse_stock("wh_mum", "item_108"), before_wh + 12)
        self.assertEqual(self.total_stock("item_108"), before_total + 12)
        self.assert_totals_consistent()

        request = database.get_restock_requests()[0]
        self.assertEqual(request["status"], "RECEIVED")
        self.assertEqual(request["next_statuses"], [])
        self.assertEqual(request["requested_by"], "alice")
        self.assertTrue(request["received_at"])

    def test_cannot_skip_approval(self):
        restock_id = self.create_request()["restock_id"]

        result = self.move(restock_id, "RECEIVED")

        self.assertFalse(result["success"])
        self.assertEqual(result["status"], "INVALID_TRANSITION")

    def test_rejected_request_cannot_be_received(self):
        restock_id = self.create_request()["restock_id"]
        self.move(restock_id, "REJECTED")

        result = self.move(restock_id, "APPROVED")

        self.assertFalse(result["success"])

    def test_duplicate_submission_creates_one_request(self):
        first = self.create_request(key="restock-key")
        second = self.create_request(key="restock-key")

        self.assertEqual(first["restock_id"], second["restock_id"])
        self.assertEqual(len(database.get_restock_requests()), 1)

    def test_validation(self):
        self.assertEqual(self.create_request(quantity=0)["status"], "INVALID_QUANTITY")

        bad_supplier = database.create_restock_request(
            self.user_id, "item_108", "wh_mum", "sup_missing", 5, self.next_key()
        )
        self.assertEqual(bad_supplier["status"], "SUPPLIER_NOT_FOUND")

        self.assertEqual(self.move(999, "APPROVED")["status"], "RESTOCK_NOT_FOUND")
        self.assertEqual(self.move(1, "TELEPORTED")["status"], "INVALID_STATUS")

    def test_expected_arrival_uses_supplier_lead_time(self):
        self.create_request()
        request = database.get_restock_requests()[0]

        self.assertEqual(request["lead_time_days"], 1)
        self.assertEqual(request["next_statuses"], ["APPROVED", "REJECTED", "CANCELLED"])
        self.assertRegex(request["expected_by"], r"^\d{4}-\d{2}-\d{2}$")


class DeliveryTests(WarehouseTestCase):
    def move(self, order_id, status):
        return database.update_delivery_status(order_id, status, self.user_id)

    def test_delivery_lifecycle_and_timeline(self):
        order_id = self.order("item_102", 2, warehouse_id="wh_blr")["order_id"]

        for status in ["PACKED", "SHIPPED", "OUT_FOR_DELIVERY", "DELIVERED"]:
            result = self.move(order_id, status)
            self.assertTrue(result["success"], result)

        delivery = database.get_deliveries()[0]
        self.assertEqual(delivery["delivery_status"], "DELIVERED")
        self.assertEqual(
            [event["status"] for event in delivery["events"]],
            ["PLACED", "PACKED", "SHIPPED", "OUT_FOR_DELIVERY", "DELIVERED"],
        )
        self.assertEqual(delivery["allocations"][0]["warehouse_id"], "wh_blr")
        self.assertEqual(delivery["next_statuses"], [])
        self.assertEqual(delivery["username"], "alice")

    def test_cannot_skip_delivery_steps(self):
        order_id = self.order("item_102", 1)["order_id"]

        result = self.move(order_id, "DELIVERED")

        self.assertFalse(result["success"])
        self.assertEqual(result["status"], "INVALID_TRANSITION")

    def test_cancel_returns_stock_to_original_warehouses(self):
        before_blr = self.warehouse_stock("wh_blr", "item_101")
        before_mum = self.warehouse_stock("wh_mum", "item_101")
        order_id = self.order("item_101", 30)["order_id"]  # 25 BLR + 5 MUM

        self.assertEqual(self.warehouse_stock("wh_blr", "item_101"), 0)

        result = self.move(order_id, "CANCELLED")

        self.assertTrue(result["success"], result)
        self.assertEqual(self.warehouse_stock("wh_blr", "item_101"), before_blr)
        self.assertEqual(self.warehouse_stock("wh_mum", "item_101"), before_mum)
        self.assertEqual(self.total_stock("item_101"), 50)
        self.assert_totals_consistent()

        # Cancelling twice must not return the stock twice.
        self.move(order_id, "CANCELLED")
        self.assertEqual(self.total_stock("item_101"), 50)

    def test_cannot_cancel_after_shipping(self):
        order_id = self.order("item_101", 1)["order_id"]
        self.move(order_id, "PACKED")
        self.move(order_id, "SHIPPED")

        result = self.move(order_id, "CANCELLED")

        self.assertFalse(result["success"])
        self.assertEqual(self.total_stock("item_101"), 49)

    def test_cancel_removes_sale_from_forecast_history(self):
        def latest_sales():
            return self.query_one(
                "SELECT units_sold FROM sales_history WHERE item_id = 'item_103' "
                "ORDER BY day_index DESC, id DESC LIMIT 1"
            )[0]

        before = latest_sales()
        order_id = self.order("item_103", 3)["order_id"]
        self.assertEqual(latest_sales(), before + 3)

        self.move(order_id, "CANCELLED")

        self.assertEqual(latest_sales(), before)

    def test_unknown_order(self):
        self.assertEqual(self.move(12345, "PACKED")["status"], "ORDER_NOT_FOUND")


if __name__ == "__main__":
    unittest.main()
