"""
End-to-end check that warehouse writes are replicated through Raft and
applied identically on every node.

The real RaftServicer, RaftLogStore, database and application server are
wired together in-process.  A three-node cluster is simulated: the leader
appends to its log, the two followers receive AppendEntries, and every
node applies the committed entries.  No gRPC network is used, so the test
runs anywhere (a minimal stand-in replaces the grpc module if it is not
installed).

Run with:

    python -m unittest tests/test_raft_warehouse_integration.py
"""

import json
import sqlite3
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
except ImportError:  # pragma: no cover
    fake_grpc = types.ModuleType("grpc")
    fake_grpc.__version__ = "99.0.0"
    fake_grpc.RpcError = type("RpcError", (Exception,), {})
    fake_grpc.insecure_channel = lambda *a, **k: None
    fake_grpc.server = lambda *a, **k: None
    fake_utilities = types.ModuleType("grpc._utilities")
    fake_utilities.first_version_is_lower = lambda *_: False
    fake_grpc._utilities = fake_utilities
    sys.modules["grpc"] = fake_grpc
    sys.modules["grpc._utilities"] = fake_utilities

import app_server  # noqa: E402
import database  # noqa: E402
import inventory_pb2  # noqa: E402
import raft_pb2  # noqa: E402
import raft_server  # noqa: E402
import seed_db  # noqa: E402
from raft_node import RaftNode  # noqa: E402


class FakeNode:
    """Just enough of RaftNode for RaftServicer and the log store."""

    def __init__(self, node_id):
        self.node_id = node_id
        self.role = "follower"
        self.current_term = 1
        self.voted_for = None
        self.leader_id = None
        self.lock = threading.RLock()
        self.last_log_index = 0
        self.last_log_term = 0

    def save_state(self):
        pass

    def become_follower(self, term):
        self.current_term = max(self.current_term, term)
        self.role = "follower"

    def receive_heartbeat(self, leader_id, term):
        self.become_follower(term)
        self.leader_id = leader_id
        return True

    def reset_election_timer(self):
        pass


class Cluster:
    """Three nodes with their own databases, one of them leader."""

    def __init__(self, root):
        self.paths = {n: Path(root) / f"node{n}.db" for n in (1, 2, 3)}
        self.servicers = {}
        self.nodes = {}

        for n, path in self.paths.items():
            with patch.object(database, "DB_PATH", path), patch("builtins.print"):
                seed_db.seed_database()
                node = FakeNode(n)
                servicer = raft_server.RaftServicer(node, f"node{n}:50052")
                self.nodes[n] = node
                self.servicers[n] = servicer

        self.leader_id = 1
        self.nodes[1].role = "leader"
        self.nodes[1].leader_id = 1
        self.nodes[1].replicate_logs = self.replicate_from_leader

    def on(self, n):
        return patch.object(database, "DB_PATH", self.paths[n])

    def replicate_from_leader(self):
        """Ship the leader's log to the followers, then commit."""
        leader = self.servicers[self.leader_id]

        with self.on(self.leader_id):
            entries = leader.log_store.get_entries(1)
            term = self.nodes[self.leader_id].current_term

        acks = 1
        for n in self.alive_followers():
            request = raft_pb2.AppendEntriesRequest(
                term=term,
                leader_id=self.leader_id,
                prev_log_index=0,
                prev_log_term=0,
                leader_commit=0,
                entries=[
                    raft_pb2.RaftLogEntry(
                        index=e["index"],
                        term=e["term"],
                        command_type=e["command_type"],
                        command_json=json.dumps(e["command"]),
                        request_id=e["request_id"] or "",
                        created_at=e["created_at"] or "",
                    )
                    for e in entries
                ],
            )
            with self.on(n), patch("builtins.print"):
                response = self.servicers[n].AppendEntries(request, None)
            if response.success:
                acks += 1

        if acks >= 2:
            last = entries[-1]["index"] if entries else 0
            with self.on(self.leader_id):
                conn = database.get_connection()
                conn.execute(
                    "UPDATE raft_metadata SET commit_index = ? WHERE singleton_id = 1",
                    (last,),
                )
                conn.commit()
                conn.close()
            for n in self.alive_followers():
                with self.on(n):
                    conn = database.get_connection()
                    conn.execute(
                        "UPDATE raft_metadata SET commit_index = ? WHERE singleton_id = 1",
                        (last,),
                    )
                    conn.commit()
                    conn.close()
                    self.servicers[n].log_store.apply_committed_entries()

        return {"success": acks >= 2}

    down = set()

    def alive_followers(self):
        return [n for n in (1, 2, 3) if n != self.leader_id and n not in self.down]

    def query(self, n, sql, params=()):
        conn = sqlite3.connect(self.paths[n])
        try:
            return conn.execute(sql, params).fetchall()
        finally:
            conn.close()


class RaftWarehouseIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.cluster = Cluster(self.temp_dir.name)

        # Application server attached to the leader, talking to its Raft
        # servicer directly instead of over gRPC.
        self.app = object.__new__(app_server.ClientServiceServicer)
        self.app.dashboard_lock = threading.Lock()
        self.app.dashboard_cache = None
        self.app.dashboard_cache_created_at = 0
        self.app.raft_stub = self.LeaderStub(self.cluster)

        with self.cluster.on(1):
            self.token = database.authenticate_user("alice", "password")
        self.key_counter = 0

    def tearDown(self):
        self.temp_dir.cleanup()

    class LeaderStub:
        def __init__(self, cluster):
            self.cluster = cluster

        def SubmitCommand(self, request, timeout=None):
            with self.cluster.on(self.cluster.leader_id), patch("builtins.print"):
                return self.cluster.servicers[self.cluster.leader_id].SubmitCommand(request, None)

    def post(self, request_type, data, key=None):
        self.key_counter += 1
        with self.cluster.on(1), patch("builtins.print"):
            return self.app.post(
                inventory_pb2.PostRequest(
                    token=self.token,
                    type=request_type,
                    data=json.dumps(data),
                    idempotency_key=key or f"k{self.key_counter}",
                ),
                None,
            )

    def stock(self, n, item_id="item_101"):
        total = self.cluster.query(n, "SELECT stock FROM products WHERE item_id=?", (item_id,))[0][0]
        rows = self.cluster.query(
            n, "SELECT warehouse_id, stock FROM warehouse_stock WHERE item_id=? ORDER BY warehouse_id", (item_id,)
        )
        return total, dict(rows)

    def assert_all_nodes_identical(self, item_id="item_101"):
        reference = self.stock(1, item_id)
        for n in (2, 3):
            if n in self.cluster.down:
                continue
            self.assertEqual(self.stock(n, item_id), reference, f"node{n} differs from leader")
        total, per_wh = reference
        self.assertEqual(sum(per_wh.values()), total, "warehouse stock must add up to total")

    # ---------------- the bug from the dashboard ----------------

    def test_ordering_more_than_a_warehouse_holds_is_rejected(self):
        # Bengaluru holds 25 of the 50 laptops.  Ordering 26 from it must fail
        # even though the total (50) would allow it.
        response = self.post("order", {"item_id": "item_101", "quantity": 26, "warehouse_id": "wh_blr"})

        self.assertEqual(response.status, "FAILED_INSUFFICIENT_STOCK", response.message)
        self.assertIn("Bengaluru Central", response.message)
        self.assertEqual(self.stock(1), (50, {"wh_blr": 25, "wh_del": 10, "wh_mum": 15}))
        self.assert_all_nodes_identical()

    def test_order_from_chosen_warehouse_replicates_allocation(self):
        response = self.post("order", {"item_id": "item_101", "quantity": 4, "warehouse_id": "wh_del"})

        self.assertEqual(response.status, "SUCCESS", response.message)
        self.assertIn("Delhi North (4)", response.message)
        self.assertEqual(self.stock(1), (46, {"wh_blr": 25, "wh_del": 6, "wh_mum": 15}))
        self.assert_all_nodes_identical()

        # Every node recorded the allocation and delivery event, not just the leader.
        for n in (1, 2, 3):
            allocations = self.cluster.query(n, "SELECT warehouse_id, quantity FROM order_allocations")
            self.assertEqual(allocations, [("wh_del", 4)], f"node{n}")
            events = self.cluster.query(n, "SELECT status FROM delivery_events")
            self.assertEqual(events, [("PLACED",)], f"node{n}")

    def test_auto_allocation_splits_identically_everywhere(self):
        response = self.post("order", {"item_id": "item_101", "quantity": 40})

        self.assertEqual(response.status, "SUCCESS", response.message)
        self.assertIn("Bengaluru Central (25), Mumbai West (15)", response.message)
        self.assertEqual(self.stock(1), (10, {"wh_blr": 0, "wh_del": 10, "wh_mum": 0}))
        self.assert_all_nodes_identical()

    def test_retry_with_same_key_is_applied_once(self):
        first = self.post("order", {"item_id": "item_101", "quantity": 3, "warehouse_id": "wh_mum"}, key="same")
        second = self.post("order", {"item_id": "item_101", "quantity": 3, "warehouse_id": "wh_mum"}, key="same")

        self.assertEqual(first.status, "SUCCESS", first.message)
        self.assertEqual(second.status, "SUCCESS", second.message)
        self.assertIn("already processed", second.message)
        self.assertEqual(self.stock(1)[1]["wh_mum"], 12)
        self.assert_all_nodes_identical()
        self.assertEqual(self.cluster.query(1, "SELECT COUNT(*) FROM raft_log")[0][0], 1)

    def test_restock_and_delivery_replicate(self):
        response = self.post(
            "restock_request",
            {"item_id": "item_108", "warehouse_id": "wh_del", "supplier_id": "sup_quickparts", "quantity": 20},
        )
        self.assertEqual(response.status, "SUCCESS", response.message)
        restock_id = self.cluster.query(1, "SELECT id FROM restock_requests")[0][0]

        for status in ["APPROVED", "SHIPPED", "RECEIVED"]:
            response = self.post("restock_status", {"restock_id": restock_id, "status": status})
            self.assertEqual(response.status, "SUCCESS", response.message)

        # Stock arrived on every node.
        self.assertEqual(self.stock(1, "item_108")[1]["wh_del"], 23)
        self.assert_all_nodes_identical("item_108")
        for n in (1, 2, 3):
            self.assertEqual(
                self.cluster.query(n, "SELECT status FROM restock_requests")[0][0], "RECEIVED", f"node{n}"
            )

        # Delivery: order 2 SSDs, cancel, stock returns everywhere.
        self.post("order", {"item_id": "item_108", "quantity": 2, "warehouse_id": "wh_del"})
        order_id = self.cluster.query(1, "SELECT id FROM orders WHERE status='SUCCESS'")[0][0]
        self.assertEqual(self.stock(1, "item_108")[1]["wh_del"], 21)

        response = self.post("delivery_status", {"order_id": order_id, "status": "CANCELLED"})
        self.assertEqual(response.status, "SUCCESS", response.message)
        self.assertEqual(self.stock(1, "item_108")[1]["wh_del"], 23)
        self.assert_all_nodes_identical("item_108")
        for n in (1, 2, 3):
            self.assertEqual(
                self.cluster.query(n, "SELECT delivery_status FROM orders WHERE id=?", (order_id,))[0][0],
                "CANCELLED",
                f"node{n}",
            )

    def test_invalid_transition_is_rejected_and_logged_identically(self):
        self.post("order", {"item_id": "item_102", "quantity": 1})
        order_id = self.cluster.query(1, "SELECT id FROM orders")[0][0]

        response = self.post("delivery_status", {"order_id": order_id, "status": "DELIVERED"})

        self.assertEqual(response.status, "INVALID_TRANSITION")
        for n in (1, 2, 3):
            self.assertEqual(
                self.cluster.query(n, "SELECT delivery_status FROM orders WHERE id=?", (order_id,))[0][0], "PLACED"
            )

    def test_follower_that_was_down_catches_up(self):
        self.cluster.down = {3}
        self.post("order", {"item_id": "item_101", "quantity": 5, "warehouse_id": "wh_blr"})
        self.assertEqual(self.stock(1)[1]["wh_blr"], 20)
        self.assertEqual(self.stock(3)[1]["wh_blr"], 25)  # missed it

        self.cluster.down = set()
        self.cluster.replicate_from_leader()  # next heartbeat/replication

        self.assertEqual(self.stock(3), self.stock(1))
        self.assert_all_nodes_identical()

    def test_non_leader_rejects_writes(self):
        self.cluster.nodes[1].role = "follower"
        response = self.post("order", {"item_id": "item_101", "quantity": 1})
        self.assertEqual(response.status, "NOT_LEADER")
        self.assertEqual(self.stock(1)[0], 50)


if __name__ == "__main__":
    unittest.main()
