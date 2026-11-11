import grpc
import raft_pb2, raft_pb2_grpc

def find_leader(addresses):
    responses = []
    for address in addresses:
        try:
            with grpc.insecure_channel(address) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel=channel)
                response = stub.GetStatus(
                    raft_pb2.StatusRequest(),
                    timeout=1
                )
                print(
                    f"{address}: "
                    f"Node={response.node_id}, "
                    f"Role={response.role}, "
                    f"Term={response.term}"
                )

                responses.append((address, response))
        except grpc.RpcError as e:
            print(f"{address}: unreachable ({e.code().name})")
            continue

    if not responses:
        print("No reachable raft node.")
        return None

    highest_term = max(response.term for _, response in responses)

    leaders = []
    for address, response in responses:
        if (
            response.term == highest_term
            and response.role == "leader"
            and response.HasField("leader_id")
            and response.node_id == response.leader_id
        ):
            leaders.append(response)

    if len(leaders) == 1:
        app_address = leaders[0].app_address
        if app_address:
            distinct = []
            for _, response in responses:
                if (response.term == leaders[0].term and
                    response.leader_id == leaders[0].leader_id
                ):
                    distinct.append(response.node_id)
            if len(distinct) >= 2:
                print("Leader inventory address:", app_address)
                return app_address
        print("Leader has no inventory address")
        return None
    elif not leaders:
        print("No leader elected yet")
    else:
        print("ERROR: Multiple leaders elected", leaders)

    return None

if __name__ == "__main__":
    find_leader(["localhost:50061", "localhost:50062", "localhost:50063"])
