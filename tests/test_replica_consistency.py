"""Writes that must have the same effect on every replica."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import database
import seed_db
from raft_replication import RaftLogStore


class ReplicaConsistencyTests(unittest.TestCase):
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
        self.user = self.conn.execute("SELECT id FROM users WHERE username='alice'").fetchone()[0]
        self.supplier = self.conn.execute("SELECT supplier_id FROM suppliers LIMIT 1").fetchone()[0]

    def apply(self, kind, data, key):
        self.store.append_local_entry(1, kind, data, key)
        self.store.advance_commit_index(1)
        self.store.apply_committed_entries()

    def stock(self, item, warehouse):
        return self.conn.execute(
            "SELECT stock FROM warehouse_stock WHERE item_id=? AND warehouse_id=?", (item, warehouse)
        ).fetchone()[0]

    def restock(self, item, warehouse, quantity, key):
        return database.create_restock_request(
            self.user, item, warehouse, self.supplier, quantity, key, created_at=f"2026-01-01T00:00:{key}"
        )["restock_id"]

    def test_restock_update_follows_fingerprint_not_local_id(self):
        # On this node ID `mice` is the mouse request; on the leader the
        # same ID named the laptop request, whose fingerprint is replicated.
        mice = self.restock("item_102", "wh_del", 3, "01")
        laptops = self.restock("item_101", "wh_blr", 25, "02")
        conn = database.get_connection()
        fingerprint = database.restock_fingerprint(conn, laptops)
        conn.close()
        before_mice, before_laptops = self.stock("item_102", "wh_del"), self.stock("item_101", "wh_blr")

        for status in ("APPROVED", "SHIPPED", "RECEIVED"):
            self.apply("UPDATE_RESTOCK", dict(restock_id=mice, new_status=status, user_id=self.user,
                                             restock=fingerprint), "r-" + status)

        self.assertEqual(self.stock("item_101", "wh_blr"), before_laptops + 25)
        self.assertEqual(self.stock("item_102", "wh_del"), before_mice)

    def test_restock_update_without_fingerprint_uses_id(self):
        mice = self.restock("item_102", "wh_del", 3, "01")
        before = self.stock("item_102", "wh_del")
        for status in ("APPROVED", "SHIPPED", "RECEIVED"):
            self.apply("UPDATE_RESTOCK", dict(restock_id=mice, new_status=status, user_id=self.user), status)
        self.assertEqual(self.stock("item_102", "wh_del"), before + 3)

    def test_unknown_fingerprint_changes_nothing(self):
        mice = self.restock("item_102", "wh_del", 3, "01")
        ghost = dict(item_id="item_101", warehouse_id="wh_blr", quantity=99, created_at="never")
        self.apply("UPDATE_RESTOCK", dict(restock_id=mice, new_status="APPROVED", user_id=self.user,
                                         restock=ghost), "ghost")
        status = self.conn.execute("SELECT status FROM restock_requests WHERE id=?", (mice,)).fetchone()[0]
        self.assertEqual(status, "REQUESTED")

    def test_delivery_cancellation_is_applied_from_log(self):
        self.apply("PLACE_ORDER", dict(user_id=self.user, item_id="item_101", quantity=2,
                                       warehouse_id="wh_blr", idempotency_key="o"), "order")
        order_id = self.conn.execute("SELECT MAX(id) FROM orders").fetchone()[0]
        before = self.stock("item_101", "wh_blr")
        self.apply("UPDATE_DELIVERY", dict(order_id=order_id, new_status="CANCELLED", user_id=self.user,
                                           note=""), "cancel")
        self.assertEqual(self.stock("item_101", "wh_blr"), before + 2)
        self.assertEqual(self.conn.execute("SELECT delivery_status FROM orders WHERE id=?",
                                           (order_id,)).fetchone()[0], "CANCELLED")


if __name__ == "__main__":
    unittest.main()
