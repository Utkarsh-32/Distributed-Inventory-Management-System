import time
import random
import threading
import grpc
import raft_pb2, raft_pb2_grpc

class RaftNode:

    def __init__(self, node_id, address, peers):
        self.node_id = node_id
        self.address = address
        self.peers = peers
        self.role = "follower"
        self.voted_for = None
        self.current_term = 0
        self.leader_id = None
        self.lock = threading.RLock()
        self.reset_election_timer()

    def start_election(self):
        with self.lock:
            self.role = "candidate"
            self.current_term += 1
            self.voted_for = self.node_id
            self.leader_id = None
            self.reset_election_timer()
            print(self.node_id, self.role, "current term:", self.current_term, "voted_for:", self.voted_for)
            return self.current_term

    def become_follower(self, new_term):
        with self.lock:
            if (self.current_term > new_term):
                print(self.node_id, self.role, self.voted_for, self.current_term)
                return
            if (self.current_term < new_term):
                self.voted_for = None
            self.current_term = new_term
            self.role = "follower"
            self.leader_id = None
            print(self.node_id, self.role, self.voted_for, self.current_term)

    def request_vote(self, candidate_id, candidate_term):
        with self.lock:
            if (candidate_term < self.current_term):
                return False
            if (candidate_term > self.current_term):
                self.become_follower(candidate_term)
            if (self.voted_for is None or self.voted_for == candidate_id):
                self.voted_for = candidate_id
                self.reset_election_timer()
                print("voted_for", self.voted_for)
                return True
            print("voted_for", self.voted_for)
            return False

    def receive_heartbeat(self, leader_id, leader_term):
        with self.lock:
            if (leader_term < self.current_term):
                return False
            self.become_follower(leader_term)
            self.leader_id = leader_id
            self.reset_election_timer()
            return True

    def reset_election_timer(self):
        with self.lock:
            self.election_deadline = time.monotonic() + random.uniform(3, 5)

    def check_election_timeout(self):
        with self.lock:
            if (self.role == "leader"):
                return
            if (time.monotonic() >= self.election_deadline):
                return self.start_election()

    def run_timer(self):
        next_heartbeat = 0
        while True:
            election_term = self.check_election_timeout()
            if election_term is not None:
                self.collect_votes(election_term)
            if time.monotonic() >= next_heartbeat:
                self.broadcast_heartbeats()
                next_heartbeat = time.monotonic() + 0.5
            time.sleep(0.1)

    def send_vote_request(self, peer_address, election_term):
        try:
            with grpc.insecure_channel(peer_address) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel=channel)
                request = raft_pb2.VoteRequest(candidate_id=self.node_id, term=election_term)
                response = stub.RequestVote(request, timeout=1)
                return response
        except grpc.RpcError:
            return None

    def collect_votes(self, election_term):
        votes = 1
        votes_needed = (len(self.peers)+1) // 2 + 1
        for peer in self.peers:
            reply = self.send_vote_request(peer_address=peer, election_term=election_term)
            if reply is None:
                continue
            with self.lock:
                if reply.term > self.current_term:
                    self.become_follower(reply.term)
                    return
                if self.role != "candidate" or self.current_term != election_term:
                    return
                if reply.term == election_term and reply.vote_granted:
                    votes += 1
                if votes >= votes_needed:
                    self.leader_id = self.node_id
                    self.role = "leader"
                    print(self.node_id, "won the election in term", self.current_term)
                    return

    def send_heartbeat(self, peer_address, leader_term):
        try:
            with grpc.insecure_channel(peer_address) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel=channel)
                request = raft_pb2.HeartbeatRequest(leader_id=self.node_id, term=leader_term)
                response = stub.Heartbeat(request=request, timeout=0.5)
                return response
        except grpc.RpcError:
            return None

    def broadcast_heartbeats(self):
        with self.lock:
            if self.role != "leader":
                return
            leader_term = self.current_term
        for peer in self.peers:
            with self.lock:
                if self.role != "leader" or self.current_term != leader_term:
                    return
            reply = self.send_heartbeat(peer_address=peer, leader_term=leader_term)
            if reply is not None:
                with self.lock:
                    if reply.term > self.current_term:
                        self.become_follower(reply.term)
                        return
            

                



