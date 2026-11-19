import time
import unittest

from partition_demo import PartitionDemo


class PartitionDemoTests(unittest.TestCase):
    def test_isolate_purchase_and_recover(self):
        demo = PartitionDemo()
        self.addCleanup(demo.close)
        self.assertTrue(demo.start("owner"))
        self.assertFalse(demo.start("other"))

        def wait_for(phase):
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                state = demo.snapshot("owner")
                self.assertNotEqual(state["phase"], "error", state)
                if state["phase"] == phase:
                    return state
                time.sleep(.15)
            self.fail(f"Timed out waiting for {phase}: {state}")

        isolated = wait_for("partitioned")
        self.assertEqual(isolated["orders"]["minority"]["status"], "NO_QUORUM")
        self.assertTrue(isolated["orders"]["majority"]["committed"])
        self.assertEqual(sorted(n["stock"] for n in isolated["nodes"]), [0, 0, 1])
        self.assertFalse(demo.reconnect("other"))
        self.assertTrue(demo.reconnect("owner"))
        restored = wait_for("complete")
        for node in restored["nodes"]:
            self.assertEqual(node["sales"], ["order:1:majority"])
            self.assertEqual(node["stock"], 0)
            self.assertEqual(node["entries"], ["order:1:majority"])
