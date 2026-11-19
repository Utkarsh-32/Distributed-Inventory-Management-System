"""RequestVote handler regression using a real temporary log and database."""
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import database
import raft_pb2
from raft_node import RaftNode
from raft_server import RaftServicer


class RequestVoteTests(unittest.TestCase):
    def test_log_freshness_and_one_vote_per_term(self):
        previous = os.getcwd()
        with tempfile.TemporaryDirectory() as directory, patch.object(
            database, 'DB_PATH', Path(directory) / 'test.db'
        ):
            os.chdir(directory)
            try:
                service = RaftServicer(RaftNode(100, 'unused', []), 'unused')
                for index in range(10):
                    service.log_store.append_local_entry(4, 'TEST', {}, f'entry-{index}')
                # Reconstruct from actual persisted metadata and log, not fake fields.
                service = RaftServicer(RaftNode(100, 'unused', []), 'unused')
                self.assertEqual((service.node.last_log_term, service.node.last_log_index), (4, 10))
                cases = [
                    (1, 5, 3, 99, False),  # Older log term despite longer log.
                    (1, 5, 4, 8, False),   # Same log term, shorter log.
                    (2, 5, 4, 10, True),
                    (2, 5, 4, 10, True),  # Same candidate may retry.
                    (3, 5, 5, 12, False), # Cannot replace an existing vote.
                    (3, 6, 5, 1, True),   # Newer log term dominates index.
                    (1, 5, 8, 20, False), # Stale election term.
                ]
                for candidate, term, log_term, log_index, expected in cases:
                    with self.subTest(candidate=candidate, term=term, log_index=log_index):
                        response = service.RequestVote(raft_pb2.VoteRequest(
                            candidate_id=candidate, term=term,
                            last_log_term=log_term, last_log_index=log_index,
                        ), None)
                        self.assertEqual(response.vote_granted, expected)
                restored = RaftServicer(RaftNode(100, 'unused', []), 'unused')
                self.assertEqual((restored.node.current_term, restored.node.voted_for), (6, 3))
            finally:
                os.chdir(previous)


if __name__ == '__main__':
    unittest.main()
