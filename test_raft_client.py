import grpc, raft_pb2_grpc, raft_pb2

channel = grpc.insecure_channel("localhost:50062")

stub = raft_pb2_grpc.RaftServiceStub(channel=channel)

response2 = stub.GetStatus(raft_pb2.StatusRequest(), timeout=1)

print(response2.node_id, response2.role, response2.term, response2.app_address)

if response2.HasField("leader_id"):
    print("leader_id:", response2.leader_id)
else:
    print("Unknown")

