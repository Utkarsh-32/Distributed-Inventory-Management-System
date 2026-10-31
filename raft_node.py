import time
import random

class RaftNode:

    def __init__(self, node_id, address, peers):
        self.node_id = node_id
        self.address = address
        self.peers = peers
        self.role = "follower"
        self.voted_for = None
        self.current_term = 0
        self.leader_id = None
        self.reset_election_timer()

    def start_election(self):
        self.role = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        self.reset_election_timer()
        print(self.node_id, self.role, "current term:", self.current_term, "voted_for:", self.voted_for)

    def become_follower(self, new_term):
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
        if (leader_term < self.current_term):
            return False
        self.become_follower(leader_term)
        self.leader_id = leader_id
        self.reset_election_timer()
        return True

    def reset_election_timer(self):
        self.election_deadline = time.monotonic() + random.uniform(3, 5)

    def check_election_timeout(self):
        if (self.role == "leader"):
            return
        if (time.monotonic() >= self.election_deadline):
            self.start_election()       
