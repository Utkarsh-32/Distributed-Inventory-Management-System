import concurrent.futures
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import database
import seed_db
from raft_replication import RaftLogStore


class AutomaticRestockTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        db_patch = patch.object(database, "DB_PATH", Path(temp.name) / "inventory.db")
        db_patch.start()
        self.addCleanup(db_patch.stop)
        with patch("builtins.print"):
            seed_db.seed_database()
        self.conn = database.get_connection()
        self.addCleanup(self.conn.close)
        self.store = RaftLogStore()
        self.item = "item_101"
        self.wh = "wh_mum"
        self.user = self.conn.execute("SELECT id FROM users WHERE username='alice'").fetchone()[0]
        self.stock(7)

    def stock(self, quantity):
        self.conn.execute("UPDATE warehouse_stock SET stock=0 WHERE item_id=?", (self.item,))
        self.conn.execute("UPDATE warehouse_stock SET stock=? WHERE warehouse_id=? AND item_id=?",
                          (quantity, self.wh, self.item))
        self.conn.execute("UPDATE products SET stock=? WHERE item_id=?", (quantity, self.item))
        self.conn.commit()

    def sale(self, qty, key="order"):
        return database.place_order(self.user, self.item, qty, key, self.wh)

    def apply(self, kind, data, key):
        self.store.append_local_entry(1, kind, data, key)
        self.store.advance_commit_index(1)
        self.store.apply_committed_entries()

    def test_boundary_retry_and_receipt(self):
        self.sale(1)
        self.assertEqual(database.get_restock_requests(), [])
        data = dict(user_id=self.user, item_id=self.item, quantity=1, warehouse_id=self.wh)
        for _ in range(2):
            self.apply("PLACE_ORDER", data, "replicated")
        request, = database.get_restock_requests()
        self.assertTrue(request["automatic"])
        self.assertEqual((request["quantity"], request["status"]), (5, "REQUESTED"))
        for status in ("APPROVED", "SHIPPED", "RECEIVED", "RECEIVED"):
            self.apply("UPDATE_RESTOCK", dict(restock_id=request["id"], new_status=status,
                                             user_id=self.user), status)
        self.assertEqual(self.conn.execute("SELECT stock FROM products WHERE item_id=?", (self.item,)).fetchone()[0], 10)
        self.sale(5, "second-cycle")
        self.assertEqual(len(database.get_restock_requests()), 2)

    def test_concurrent_low_stock_sales(self):
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            results = list(pool.map(lambda k: self.sale(2, k), ("a", "b")))
        self.assertTrue(all(r["success"] for r in results))
        self.assertEqual(len(database.get_restock_requests()), 1)

    def test_existing_manual_request_and_cancel(self):
        supplier = self.conn.execute("SELECT supplier_id FROM suppliers LIMIT 1").fetchone()[0]
        manual = database.create_restock_request(self.user, self.item, self.wh, supplier, 3, "manual")
        self.sale(2)
        request, = database.get_restock_requests()
        self.assertFalse(request["automatic"])
        database.update_restock_status(manual["restock_id"], "CANCELLED", self.user)
        self.sale(1, "again")
        self.assertTrue(database.get_restock_requests()[0]["automatic"])

    def test_transfer_to_zero_and_rejected_transfer(self):
        data = dict(item_id=self.item, source=self.wh, destination="wh_del", quantity=8)
        self.apply("TRANSFER_STOCK", data, f"transfer:{self.user}:fail")
        self.assertEqual(database.get_restock_requests(), [])
        data["quantity"] = 7
        self.apply("TRANSFER_STOCK", data, f"transfer:{self.user}:ok")
        request, = database.get_restock_requests()
        self.assertEqual((request["quantity"], request["warehouse_id"]), (10, self.wh))

    def test_split_order_and_supplier_choice(self):
        self.conn.execute("UPDATE suppliers SET lead_time_days=3")
        self.conn.execute("UPDATE warehouse_stock SET stock=7 WHERE warehouse_id='wh_del' AND item_id=?", (self.item,))
        self.conn.execute("UPDATE products SET stock=14 WHERE item_id=?", (self.item,))
        self.conn.commit()
        supplier = self.conn.execute("SELECT min(supplier_id) FROM suppliers").fetchone()[0]
        database.place_order(self.user, self.item, 12, "split")
        requests = database.get_restock_requests()
        self.assertEqual(sorted(r["quantity"] for r in requests), [9, 9])
        self.assertEqual({r["supplier_id"] for r in requests}, {supplier})

    def test_confirmed_checkout_shortage_is_replicated_once(self):
        self.stock(17)
        data = dict(item_id=self.item, quantity=20, confirmed_shortage=3, user_id=self.user)
        for key in ("first", "first", "second"):
            self.apply("CHECKOUT_SHORTAGE", data, key)
        request, = database.get_restock_requests()
        self.assertEqual(request["quantity"], 3)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0], 0)
        self.assertEqual(self.conn.execute("SELECT stock FROM products WHERE item_id=?", (self.item,)).fetchone()[0], 17)
        self.apply("CHECKOUT_SHORTAGE", dict(data, quantity=22, confirmed_shortage=5), "partial")
        self.assertEqual(sorted(r["quantity"] for r in database.get_restock_requests()), [2, 3])

    def test_checkout_shortage_rechecks_availability_and_consent(self):
        self.stock(15)
        data = dict(item_id=self.item, quantity=20, confirmed_shortage=3, user_id=self.user)
        self.apply("CHECKOUT_SHORTAGE", data, "stale")
        self.assertEqual(database.get_restock_requests(), [])
        self.stock(20)
        self.apply("CHECKOUT_SHORTAGE", data, "enough")
        self.assertEqual(database.get_restock_requests(), [])

    def test_no_supplier_or_failed_order(self):
        self.assertFalse(self.sale(8)["success"])
        self.assertEqual(database.get_restock_requests(), [])
        self.conn.execute("DELETE FROM suppliers")
        self.conn.commit()
        self.assertTrue(self.sale(7, "sold")["success"])
        self.assertEqual(database.get_restock_requests(), [])
