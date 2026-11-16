"""
Tests for the live cluster view's backend (cluster_monitor.py).

Node statuses are supplied by a fake query function, so no Raft nodes or
network are needed.

Run with:

    python -m unittest tests/test_cluster_monitor.py
"""

import sys
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    import grpc  # noqa: F401
except ImportError:  # pragma: no cover
    fake_grpc = types.ModuleType("grpc")
    fake_grpc.__version__ = "99.0.0"
    fake_grpc.RpcError = type("RpcError", (Exception,), {})
    fake_grpc.insecure_channel = lambda *a, **k: None
    fake_utilities = types.ModuleType("grpc._utilities")
    fake_utilities.first_version_is_lower = lambda *_: False
    fake_grpc._utilities = fake_utilities
    sys.modules["grpc"] = fake_grpc
    sys.modules["grpc._utilities"] = fake_utilities

from cluster_monitor import ClusterMonitor, agreed_leader  # noqa: E402


ADDRESSES = ["node1:50061", "node2:50061", "node3:50061"]


def up(node_id, role, term, leader_id, log=5, commit=5, applied=5):
    return {
        "address": ADDRESSES[node_id - 1],
        "reachable": True,
        "node_id": node_id,
        "role": role,
        "term": term,
        "leader_id": leader_id,
        "app_address": f"node{node_id}:50052",
        "last_log_index": log,
        "commit_index": commit,
        "last_applied": applied,
        "voted_for": leader_id,
    }


def down(node_id):
    return {"address": ADDRESSES[node_id - 1], "reachable": False}


class ScriptedCluster:
    """Returns a scripted status per address; swap the script per step."""

    def __init__(self, statuses):
        self.set(statuses)

    def set(self, statuses):
        self.by_address = {status["address"]: status for status in statuses}

    def __call__(self, address):
        return self.by_address[address]


class AgreedLeaderTests(unittest.TestCase):
    def test_leader_confirmed_by_majority(self):
        nodes = [up(1, "follower", 3, 2), up(2, "leader", 3, 2), up(3, "follower", 3, 2)]
        self.assertEqual(agreed_leader(nodes), 2)

    def test_leader_needs_second_confirmation(self):
        nodes = [up(1, "follower", 3, None), up(2, "leader", 3, 2), up(3, "follower", 3, None)]
        self.assertIsNone(agreed_leader(nodes))

    def test_stale_leader_in_old_term_is_ignored(self):
        nodes = [up(1, "leader", 2, 1), up(2, "leader", 3, 2), up(3, "follower", 3, 2)]
        self.assertEqual(agreed_leader(nodes), 2)

    def test_all_down(self):
        self.assertIsNone(agreed_leader([down(1), down(2), down(3)]))


class ClusterMonitorTests(unittest.TestCase):
    def test_healthy_snapshot(self):
        script = ScriptedCluster([up(1, "leader", 1, 1), up(2, "follower", 1, 1), up(3, "follower", 1, 1)])
        monitor = ClusterMonitor(ADDRESSES, query=script)

        snap = monitor.snapshot()

        self.assertTrue(snap["healthy"])
        self.assertEqual(snap["leader_id"], 1)
        self.assertEqual(snap["term"], 1)
        self.assertEqual(snap["reachable_count"], 3)
        self.assertEqual(snap["quorum"], 2)
        self.assertEqual([n["node_id"] for n in snap["nodes"]], [1, 2, 3])
        self.assertEqual(snap["events"][0]["message"], "node1 is leader in term 1")

    def test_failover_produces_events_in_order(self):
        script = ScriptedCluster([up(1, "leader", 1, 1), up(2, "follower", 1, 1), up(3, "follower", 1, 1)])
        monitor = ClusterMonitor(ADDRESSES, query=script)
        monitor.snapshot()

        # Leader dies; followers have not noticed yet.
        script.set([down(1), up(2, "follower", 1, 1), up(3, "follower", 1, 1)])
        snap = monitor.snapshot()
        self.assertFalse(snap["healthy"])
        self.assertIsNone(snap["leader_id"])
        messages = [e["message"] for e in snap["events"]]
        self.assertIn("node1 stopped responding", messages)
        self.assertIn("no agreed leader, election in progress", messages)

        # node3 wins term 2.
        script.set([down(1), up(2, "follower", 2, 3), up(3, "leader", 2, 3)])
        snap = monitor.snapshot()
        self.assertTrue(snap["healthy"])
        self.assertEqual(snap["leader_id"], 3)
        newest = [e["message"] for e in snap["events"][:2]]
        self.assertIn("node3 elected leader in term 2", newest)
        self.assertIn("term advanced 1 → 2 (election)", newest)

        # node1 returns as a follower, behind on the log.
        script.set([up(1, "follower", 2, 3, log=3, applied=3), up(2, "follower", 2, 3), up(3, "leader", 2, 3)])
        snap = monitor.snapshot()
        self.assertEqual(snap["events"][0]["message"], "node1 is back as follower in term 2")
        self.assertEqual(snap["nodes"][0]["last_applied"], 3)

    def test_down_nodes_are_listed_and_quorum_loss_is_unhealthy(self):
        script = ScriptedCluster([up(1, "leader", 1, 1), down(2), down(3)])
        monitor = ClusterMonitor(ADDRESSES, query=script)

        snap = monitor.snapshot()

        self.assertEqual(snap["reachable_count"], 1)
        self.assertFalse(snap["healthy"])
        self.assertEqual(len(snap["nodes"]), 3)
        self.assertFalse(snap["nodes"][1]["reachable"])

    def test_event_feed_is_bounded(self):
        script = ScriptedCluster([up(1, "leader", 1, 1), up(2, "follower", 1, 1), up(3, "follower", 1, 1)])
        monitor = ClusterMonitor(ADDRESSES, query=script)
        monitor.snapshot()

        for i in range(40):
            script.set([up(1, "leader", 1, 1), up(2, "follower", 1, 1), down(3)] if i % 2 else
                       [up(1, "leader", 1, 1), up(2, "follower", 1, 1), up(3, "follower", 1, 1)])
            monitor.snapshot()

        self.assertLessEqual(len(monitor.snapshot()["events"]), 25)


if __name__ == "__main__":
    unittest.main()
