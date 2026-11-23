import threading
import time
import unittest

from raft_server import RaftServicer


class FakeNode:
    """replicate_logs() reports busy while a heartbeat pass holds the lock."""
    def __init__(self, committed_by_own_pass):
        self.replication_lock = threading.Lock()
        self.committed_by_own_pass = committed_by_own_pass
        self.own_passes = 0

    def replicate_logs(self):
        if not self.replication_lock.acquire(blocking=False):
            return {"success": False, "reason": "REPLICATION_ALREADY_RUNNING"}
        try:
            self.own_passes += 1
            return {"success": True} if self.committed_by_own_pass else {"success": False, "reason": "NO_MAJORITY"}
        finally:
            self.replication_lock.release()


def servicer_for(node):
    servicer = RaftServicer.__new__(RaftServicer)
    servicer.node = node
    return servicer


def hold_lock(node, seconds):
    node.replication_lock.acquire()
    threading.Timer(seconds, node.replication_lock.release).start()


class ReplicationWaitTests(unittest.TestCase):
    def test_waits_for_running_heartbeat_then_replicates(self):
        node = FakeNode(committed_by_own_pass=True)
        hold_lock(node, .3)
        started = time.monotonic()
        result = servicer_for(node).replicate_new_entry()
        self.assertEqual(result, {"success": True})
        self.assertEqual(node.own_passes, 1)
        self.assertGreaterEqual(time.monotonic() - started, .25)

    def test_real_pass_without_majority_returns_immediately(self):
        node = FakeNode(committed_by_own_pass=False)
        started = time.monotonic()
        result = servicer_for(node).replicate_new_entry()
        self.assertEqual(result["reason"], "NO_MAJORITY")
        self.assertLess(time.monotonic() - started, .1)

    def test_gives_up_after_wait_limit(self):
        node = FakeNode(committed_by_own_pass=True)
        servicer = servicer_for(node)
        servicer.REPLICATION_WAIT_SECONDS = .2
        hold_lock(node, .6)
        result = servicer.replicate_new_entry()
        self.assertEqual(result["reason"], "REPLICATION_ALREADY_RUNNING")
        self.assertEqual(node.own_passes, 0)
        time.sleep(.5)


if __name__ == "__main__":
    unittest.main()
