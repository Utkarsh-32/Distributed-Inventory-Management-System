from raft_node import RaftNode
import time

node1 = RaftNode(1, "localhost:50061", ["localhost:50062","localhost:50063"])

node2 = RaftNode(2, "localhost:50062", ["localhost:50061","localhost:50063"])

node3 = RaftNode(3, "localhost:50063", ["localhost:50061","localhost:50062"])

node1.start_election()

votes = 1

if (node2.request_vote(node1.node_id, node1.current_term)):
    votes += 1

if (node3.request_vote(node1.node_id, node1.current_term)):
    votes += 1

if (votes >= 2):
    node1.role = "leader"
    node1.leader_id = node1.node_id

    node2.receive_heartbeat(node1.node_id, node1.current_term)

    node3.receive_heartbeat(node1.node_id, node1.current_term)

    time.sleep(6)

    node2.check_election_timeout()

    new_votes = 1

    if (node3.request_vote(node2.node_id, node2.current_term)):
        new_votes += 1

    if new_votes >= 2:
        node2.role = "leader"
        node2.leader_id = node2.node_id
        node3.receive_heartbeat(node2.node_id, node2.current_term)
        node1.receive_heartbeat(node2.node_id, node2.current_term)


    print(node2.role, node2.current_term)
    print(node1.role, node1.current_term)