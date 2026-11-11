
import grpc
import raft_pb2
import raft_pb2_grpc

# Addresses of your three Raft nodes
addresses = [
    "localhost:50061",
    "localhost:50062",
    "localhost:50063"
]

for address in addresses:
    print(f"\nChecking {address}")

    try:
        # 1. Connect to the node
        with grpc.insecure_channel(address) as channel:

            # 2. Create a stub
            stub = raft_pb2_grpc.RaftServiceStub(channel)

            # 3. Request the node's status
            response = stub.GetStatus(
                raft_pb2.StatusRequest(),
                timeout=1
            )

            # 4. Print the node's information
            print("Node ID:", response.node_id)
            print("Role:", response.role)
            print("Term:", response.term)

            # 5. Check whether leader ID is known
            if response.HasField("leader_id"):
                print("Leader ID:", response.leader_id)
            else:
                print("Leader ID: Unknown")

    except grpc.RpcError:
        print("unreachable")
