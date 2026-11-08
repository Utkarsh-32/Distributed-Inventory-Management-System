import raft_pb2, raft_pb2_grpc
import grpc
from concurrent import futures
from raft_node import RaftNode
import argparse
import threading

class RaftServicer(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node):
        self.node = node

    def RequestVote(self, request, context):
        with self.node.lock:
            granted = self.node.request_vote(request.candidate_id, request.term)
            return raft_pb2.VoteResponse(term=self.node.current_term, vote_granted=granted)

    def Heartbeat(self, request, context):
        with self.node.lock:
            accepted = self.node.receive_heartbeat(request.leader_id, request.term)
            return raft_pb2.HeartbeatResponse(term=self.node.current_term, accepted=accepted)


def serve(node_id, address, peers):
    node = RaftNode(node_id=node_id, address=address, peers=peers)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    servicer = RaftServicer(node)
    raft_pb2_grpc.add_RaftServiceServicer_to_server(servicer=servicer, server=server)
    server.add_insecure_port(node.address)
    server.start()
    timer_thread = threading.Thread(target=node.run_timer, daemon=True)
    print("node_id:", node.node_id, "address:", node.address)
    timer_thread.start()
    server.wait_for_termination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-id", type=int, required=True)
    parser.add_argument("--address", required=True)
    parser.add_argument("--peers", nargs="+", required=True)
    args = parser.parse_args()
    serve(args.node_id, args.address, args.peers)
