"""Isolated persisted-vote regression; no live node or shared data required."""
import os
import tempfile
import unittest
from raft_node import RaftNode


class VotePersistenceTests(unittest.TestCase):
    def test_vote_survives_reload_and_changes_only_in_new_term(self):
        previous = os.getcwd()
        with tempfile.TemporaryDirectory() as directory:
            os.chdir(directory)
            try:
                node = RaftNode(100, 'unused', [])
                self.assertTrue(node.request_vote(1, 5, 0, 0))
                restored = RaftNode(100, 'unused', [])
                self.assertEqual((restored.current_term, restored.voted_for), (5, 1))
                self.assertFalse(restored.request_vote(2, 5, 0, 0))
                self.assertTrue(restored.request_vote(1, 5, 0, 0))
                self.assertFalse(restored.request_vote(2, 4, 0, 0))
                self.assertTrue(restored.request_vote(2, 6, 0, 0))
                again = RaftNode(100, 'unused', [])
                self.assertEqual((again.current_term, again.voted_for), (6, 2))
            finally:
                os.chdir(previous)


if __name__ == '__main__':
    unittest.main()
