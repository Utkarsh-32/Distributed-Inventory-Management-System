import raft_pb2, raft_pb2_grpc
import grpc
from concurrent import futures
from raft_node import RaftNode

class RaftServicer(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node):
        self.node = node

    def RequestVote(self, request, context):
        granted = self.node.request_vote(request.candidate_id, request.term)
        return raft_pb2.VoteResponse(term=self.node.current_term, vote_granted=granted)

    def Heartbeat(self, request, context):
        accepted = self.node.receive_heartbeat(request.leader_id, request.term)
        return raft_pb2.HeartbeatResponse(term=self.node.current_term, accepted=accepted)


def serve():
    node2 = RaftNode(2, "localhost:50062", ["localhost:50061"])
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    servicer = RaftServicer(node2)
    raft_pb2_grpc.add_RaftServiceServicer_to_server(servicer=servicer, server=server)
    server.add_insecure_port(node2.address)
    server.start()
    print("node2 listening")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
