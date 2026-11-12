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

    leaders = dict()
    for address, response in responses:
        if (
            response.term == highest_term
            and response.role == "leader"
            and response.HasField("leader_id")
            and response.node_id == response.leader_id
        ):
            leaders[response.node_id] = response

    if len(leaders) == 1:
        leader_id, leader_response = next(iter(leaders.items()))
        if leader_response:
            distinct = set()
            for _, response in responses:
                if (response.HasField("leader_id") and response.term == leader_response.term and
                    response.leader_id == leader_id
                ):
                    distinct.add(response.node_id)
            if len(distinct) >= 2:
                print("Leader inventory address:", leader_response.app_address)
                return leader_response.app_address
            elif len(distinct) < 2:
                print("Waiting for majority to confirm the leader.")
        return None
    elif not leaders:
        print("No leader elected yet")
    else:
        print("ERROR: Multiple leaders elected", leaders)

    return None

if __name__ == "__main__":
    find_leader(["localhost:50061", "localhost:50061", "localhost:50065"])
