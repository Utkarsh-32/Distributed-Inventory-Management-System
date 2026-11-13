from raft_node import RaftNode

node = RaftNode(100, "localhost:50066", [])

result = node.request_vote(1, 5)

print("term:", node.current_term, "voted:", result)

node = RaftNode(100, "localhost:50066", [])

print("term:", node.current_term, "voted_for:", node.voted_for)

result1 = node.request_vote(2, 5)

print("term:", 5, "voted:", result1)

result2 = node.request_vote(2, 6)

print("term:", node.current_term, "voted:", result2)