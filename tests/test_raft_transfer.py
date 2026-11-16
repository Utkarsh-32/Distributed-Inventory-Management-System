import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import database
from raft_replication import RaftLogStore, RequestIdConflict


class TransferTest(unittest.TestCase):
    def test_transfer_retry_and_rejection(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(
            database, "DB_PATH", Path(directory) / "test.db"
        ):
            store = RaftLogStore()
            conn = database.get_connection()
            self.addCleanup(conn.close)
            conn.execute("INSERT INTO products VALUES ('item','Test product',25)")
            conn.executemany("INSERT INTO warehouses (warehouse_id,name) VALUES (?,?)",
                             [('a', 'Source'), ('b', 'Destination')])
            conn.execute("INSERT INTO warehouse_stock VALUES ('a','item',25)")
            conn.commit()

            def submit(key, quantity):
                store.append_local_entry(1, 'TRANSFER_STOCK', dict(
                    item_id='item', source='a', destination='b', quantity=quantity
                ), key)
                store.advance_commit_index(1)
                store.apply_committed_entries()

            submit('too-many', 26)
            submit('move', 10)
            submit('move', 10)
            with self.assertRaises(RequestIdConflict):
                submit('move', 11)
            self.assertEqual(
                [row[0] for row in conn.execute("SELECT stock FROM warehouse_stock ORDER BY warehouse_id")],
                [15, 10],
            )
            self.assertEqual(conn.execute("SELECT stock FROM products").fetchone()[0], 25)
            self.assertEqual(
                [row[0] for row in conn.execute("SELECT status FROM warehouse_transfers ORDER BY rowid")],
                ['FAILED_INSUFFICIENT_STOCK', 'SUCCESS'],
            )
