import time
import random
import threading
import grpc
import raft_pb2, raft_pb2_grpc
import json
from pathlib import Path
import os

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
        self.replication_lock = threading.Lock()
        data_dir = Path("data")
        data_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = data_dir / f"node_{self.node_id}_state.json"
        self.load_state()
        self.reset_election_timer()
        self.last_log_term = 0
        self.last_log_index = 0

    def start_election(self):
        with self.lock:
            self.role = "candidate"
            self.current_term += 1
            self.voted_for = self.node_id
            self.leader_id = None
            self.reset_election_timer()
            self.save_state()
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
                self.save_state()
            self.role = "follower"
            self.leader_id = None
            print(self.node_id, self.role, self.voted_for, self.current_term)

    def request_vote(self, candidate_id, candidate_term, candidate_last_term, candidate_last_index):
        with self.lock:
            if (candidate_term < self.current_term):
                return False
            if (candidate_term > self.current_term):
                self.become_follower(candidate_term)
            if (self.voted_for is None or self.voted_for == candidate_id):
                if not self.is_log_up_to_date(candidate_last_term, candidate_last_index, self.last_log_term, self.last_log_index):
                    return False
                self.voted_for = candidate_id
                self.save_state()
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
                try:
                    self.replicate_logs()
                except Exception as exc:
                    print(
                        "Node",
                        self.node_id,
                        "replication error:",
                        exc,
                        flush=True
                    )
                next_heartbeat = time.monotonic() + 0.5
            time.sleep(0.1)

    def send_vote_request(self, peer_address, election_term):
        try:
            with grpc.insecure_channel(peer_address) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel=channel)
                request = raft_pb2.VoteRequest(candidate_id=self.node_id, term=election_term, last_log_index=self.last_log_index, last_log_term=self.last_log_term)
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

                    # Reset old follower acknowledgements.
                    if hasattr(self, "log_store"):
                        self.log_store.reset_replication_progress()

                        last_info = self.log_store.get_last_log_info()
                        self.last_log_index = last_info["last_log_index"]
                        self.last_log_term = last_info["last_log_term"]

                    print(
                        self.node_id,
                        "won the election in term",
                        self.current_term
                    )

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

    def send_append_entries(self, peer_address, request_data):

        with self.lock:
            if (
                self.role != "leader"
                or self.current_term != request_data["term"]
            ):
                return {
                    "success": False,
                    "term": self.current_term,
                    "reason": "NOT_LEADER"
                }

        protobuf_entries = []

        for entry in request_data["entries"]:

            protobuf_entries.append(
                raft_pb2.RaftLogEntry(
                    index=entry["index"],
                    term=entry["term"],
                    command_type=entry["command_type"],
                    command_json=json.dumps(
                        entry["command"],
                        sort_keys=True,
                        separators=(",", ":")
                    ),
                    request_id=entry.get("request_id") or "",
                    created_at=entry.get("created_at") or ""
                )
            )

        request = raft_pb2.AppendEntriesRequest(
            leader_id=self.node_id,
            term=request_data["term"],
            prev_log_index=request_data["prev_log_index"],
            prev_log_term=request_data["prev_log_term"],
            entries=protobuf_entries,
            leader_commit=request_data["leader_commit"]
        )

        try:
            with grpc.insecure_channel(peer_address) as channel:

                stub = raft_pb2_grpc.RaftServiceStub(
                    channel=channel
                )

                response = stub.AppendEntries(
                    request,
                    timeout=1.5
                )

                return {
                    "success": response.success,
                    "term": response.term,
                    "match_index": response.match_index,
                    "conflict_index": response.conflict_index,
                    "reason": response.reason
                }

        except grpc.RpcError as error:
            raise RuntimeError(
                f"AppendEntries failed for {peer_address}: {error}"
            ) from error

    def replicate_logs(self):

        if not hasattr(self, "log_store"):
            raise RuntimeError("Raft log store is not initialized.")

        if not self.replication_lock.acquire(blocking=False):
            return {
                "success": False,
                "reason": "REPLICATION_ALREADY_RUNNING"
            }

        try:
            with self.lock:

                if self.role != "leader":
                    return {
                        "success": False,
                        "reason": "NOT_LEADER"
                    }

                leader_term = self.current_term
                peers = list(self.peers)

            state = self.log_store.get_state()

            if state["current_term"] > leader_term:

                with self.lock:
                    self.become_follower(state["current_term"])

                return {
                    "success": False,
                    "reason": "HIGHER_TERM_DISCOVERED"
                }

            if state["current_term"] < leader_term:

                self.log_store.save_election_state(
                    leader_term,
                    self.voted_for
                )

            follower_callbacks = {
                peer: (
                    lambda request, address=peer:
                    self.send_append_entries(address, request)
                )
                for peer in peers
            }

            replication_result = (
                self.log_store.replicate_to_followers(
                    follower_callbacks
                )
            )

            if not replication_result["leadership_valid"]:

                higher_term = replication_result["higher_term"]

                with self.lock:
                    if higher_term > self.current_term:
                        self.become_follower(higher_term)

                return {
                    "success": False,
                    "reason": "HIGHER_TERM_DISCOVERED",
                    "higher_term": higher_term,
                    "replication": replication_result
                }

            with self.lock:
                if (
                    self.role != "leader"
                    or self.current_term != leader_term
                ):
                    return {
                        "success": False,
                        "reason": "LEADERSHIP_CHANGED"
                    }

            commit_result = (
                self.log_store.advance_commit_index(
                    cluster_size=len(peers) + 1
                )
            )

            applied_entries = []
            application_error = None

            try:
                applied_entries = (
                    self.log_store.apply_committed_entries()
                )
            except Exception as exc:
                application_error = str(exc)

            last_info = self.log_store.get_last_log_info()

            with self.lock:
                self.last_log_index = last_info["last_log_index"]
                self.last_log_term = last_info["last_log_term"]

                if (
                    self.role != "leader"
                    or self.current_term != leader_term
                ):
                    return {
                        "success": False,
                        "reason": "LEADERSHIP_CHANGED"
                    }

            commit_propagation = None

            if commit_result["committed"]:

                commit_propagation = (
                    self.log_store.replicate_to_followers(
                        follower_callbacks
                    )
                )

                if not commit_propagation["leadership_valid"]:

                    higher_term = commit_propagation["higher_term"]

                    with self.lock:
                        if higher_term > self.current_term:
                            self.become_follower(higher_term)

            return {
                "success": True,
                "term": leader_term,
                "replication": replication_result,
                "commit": commit_result,
                "applied_entries": applied_entries,
                "application_error": application_error,
                "commit_propagation": commit_propagation
            }

        finally:
            self.replication_lock.release()


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


    def save_state(self):
        with self.lock:
            try:
                state = {
                    "current_term" : self.current_term,
                    "voted_for" : self.voted_for
                }
                temp_file = self.state_file.with_suffix(".tmp")

                with open(temp_file, "w") as f:
                    json.dump(state, f)
                    f.flush()
                    os.fsync(f.fileno())

                os.replace(temp_file, self.state_file)
                if hasattr(self, "log_store"):
                    self.log_store.save_election_state(
                        self.current_term,
                        self.voted_for
                    )

                if os.name != "nt":
                    dir_fd = os.open(self.state_file.parent, os.O_RDONLY)

                    try:
                        os.fsync(dir_fd)
                    finally:
                        os.close(dir_fd)

            except OSError as error:
                print("node_id:", self.node_id, "error:", error, flush=True)
                os._exit(1)

    def load_state(self):
        with self.lock:
            if not self.state_file.exists():
                return
            with open(self.state_file, "r") as f:
                state = json.load(f)
            if not isinstance(state, dict):
                raise ValueError("Value must be a JSON object.")
            current_term = state["current_term"]
            voted_for = state["voted_for"]
            if (type(current_term) is int and current_term >= 0) and (voted_for is None or type(voted_for) is int):
                self.current_term = current_term
                self.voted_for = voted_for
            else:
                raise ValueError("Failed to read the values")

    def is_log_up_to_date(self, candidate_last_term, candidate_last_index, local_last_term, local_last_index):
        if candidate_last_term > local_last_term:
            return True
        if candidate_last_term < local_last_term:
            return False
        if candidate_last_index >= local_last_index:
            return True
        if candidate_last_index < local_last_index:
            return False