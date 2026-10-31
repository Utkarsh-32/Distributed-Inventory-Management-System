import grpc, raft_pb2_grpc, raft_pb2

channel = grpc.insecure_channel("localhost:50062")

stub = raft_pb2_grpc.RaftServiceStub(channel=channel)

request = raft_pb2.VoteRequest(candidate_id=1, term=1)

response = stub.RequestVote(request, timeout=3)

request1 = raft_pb2.HeartbeatRequest(leader_id=1, term=1)

response1 = stub.Heartbeat(request1, timeout=3)

print(response1.term, response1.accepted)
