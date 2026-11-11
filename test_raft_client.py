import grpc, raft_pb2_grpc, raft_pb2

channel = grpc.insecure_channel("localhost:50062")

stub = raft_pb2_grpc.RaftServiceStub(channel=channel)

request = raft_pb2.VoteRequest(candidate_id=1, term=1)

response = stub.RequestVote(request, timeout=3)

request1 = raft_pb2.HeartbeatRequest(leader_id=1, term=1)

response1 = stub.Heartbeat(request1, timeout=3)

print(response1.term, response1.accepted)

response2 = stub.GetStatus(raft_pb2.StatusRequest(), timeout=1)

print(response2.node_id, response2.role, response2.term, response2.app_address)

if response2.HasField("leader_id"):
    print("leader_id:", response2.leader_id)
else:
    print("Unknown")

