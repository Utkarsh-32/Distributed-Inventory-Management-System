"""Run isolated regressions; exclude opt-in live-service/order scripts."""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests')]
MODULES = [
    'test_warehouse_features', 'test_automatic_restock', 'test_raft_transfer',
    'test_order_forecast_history', 'test_dashboard_optimization',
    'test_cluster_monitor', 'test_warehouse_rpc', 'test_partition_demo',
    'test_raft_persistence', 'test_raft_request_vote', 'test_local_election',
    'test_replication_wait',
]

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromNames(MODULES)
    )
    sys.exit(not result.wasSuccessful())
