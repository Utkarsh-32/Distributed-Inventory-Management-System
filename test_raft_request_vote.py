
# test_raft_request_vote.py

from types import SimpleNamespace
from raft_node import RaftNode
from raft_server import RaftServicer

# Create a follower for testing
follower = RaftNode(100, "localhost:50066", [])

# Set initial state
with follower.lock:
    follower.current_term = 5
    follower.voted_for = None
    follower.last_log_term = 4
    follower.last_log_index = 10

    # IMPORTANT:
    # Configure follower's actual log to have
    # last_log_term = 4 and last_log_index = 10.
    # How to do this depends on your log structure.

service = RaftServicer(follower)

cases = [
    (1, 5, 4, 8, False),
    (2, 5, 4, 10, True),
    (3, 5, 5, 12, False),
    (3, 6, 5, 12, True),
]

for candidate, term, log_term, log_index, expected in cases:
    request = SimpleNamespace(
        candidate_id=candidate,
        term=term,
        last_log_term=log_term,
        last_log_index=log_index,
    )

    response = service.RequestVote(request, None)
    actual = response.vote_granted

    print(
        f"Candidate {candidate}, term {term}: "
        f"Expected={expected}, Actual={actual}"
    )

    assert actual == expected, "FAILED"

print("All RequestVote tests passed!")
