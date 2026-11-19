"""Deterministic in-process election check; transport is mocked, not a live failure test."""
import os
import tempfile
import unittest
from unittest.mock import patch
import raft_pb2
from raft_node import RaftNode


class LocalElectionTests(unittest.TestCase):
    def test_majority_election_and_higher_term_step_down(self):
        previous = os.getcwd()
        with tempfile.TemporaryDirectory() as directory:
            os.chdir(directory)
            try:
                nodes = {str(n): RaftNode(n, str(n), [str(p) for p in (1, 2, 3) if p != n])
                         for n in (1, 2, 3)}

                def elect(candidate, unavailable=()):
                    term = candidate.start_election()
                    def reply(peer_address, election_term):
                        if peer_address in unavailable:
                            return None
                        peer = nodes[peer_address]
                        granted = peer.request_vote(candidate.node_id, election_term,
                                                    candidate.last_log_term, candidate.last_log_index)
                        return raft_pb2.VoteResponse(term=peer.current_term, vote_granted=granted)
                    with patch.object(candidate, 'send_vote_request', side_effect=reply):
                        candidate.collect_votes(term)
                    return term

                first_term = elect(nodes['1'])
                self.assertEqual(nodes['1'].role, 'leader')
                for peer in ('2', '3'):
                    nodes[peer].receive_heartbeat(1, first_term)
                next_term = elect(nodes['2'], unavailable=('1',))
                self.assertGreater(next_term, first_term)
                self.assertEqual(nodes['2'].role, 'leader')
                nodes['1'].receive_heartbeat(2, next_term)
                self.assertEqual((nodes['1'].role, nodes['1'].leader_id), ('follower', 2))
                elect(nodes['3'], unavailable=('1', '2'))
                self.assertEqual(nodes['3'].role, 'candidate')
            finally:
                os.chdir(previous)


if __name__ == '__main__':
    unittest.main()
