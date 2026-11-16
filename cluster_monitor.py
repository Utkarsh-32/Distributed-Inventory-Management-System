"""
Live view of the Raft cluster for the dashboard.

``ClusterMonitor.snapshot()`` asks every Raft node for its status in
parallel and returns a JSON-friendly summary: each node's role, term,
leader, log progress and reachability, plus the cluster's agreed leader and
a short feed of recent events (elections, nodes going down or coming back,
term changes).  The dashboard polls it every couple of seconds, so an
election started by stopping a container is visible on the page as it
happens.

The monitor keeps the previous snapshot only to derive events; it never
influences the cluster.
"""

import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import grpc

import raft_pb2
import raft_pb2_grpc


STATUS_TIMEOUT_SECONDS = 1.0
MAX_EVENTS = 25


def query_node(address, timeout=STATUS_TIMEOUT_SECONDS):
    """Return one node's status as a dict, or a 'down' record."""
    try:
        with grpc.insecure_channel(address) as channel:
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            response = stub.GetStatus(raft_pb2.StatusRequest(), timeout=timeout)

    except grpc.RpcError:
        return {"address": address, "reachable": False}

    return {
        "address": address,
        "reachable": True,
        "node_id": response.node_id,
        "role": response.role,
        "term": response.term,
        "leader_id": response.leader_id if response.HasField("leader_id") else None,
        "app_address": response.app_address,
        "last_log_index": response.last_log_index,
        "commit_index": response.commit_index,
        "last_applied": response.last_applied,
        "voted_for": response.voted_for or None,
    }


def agreed_leader(nodes):
    """
    The leader the cluster agrees on, using the same rule as
    leader_discovery.find_leader: a single node reporting itself leader in
    the highest term, confirmed by at least two distinct nodes.
    """
    reachable = [node for node in nodes if node["reachable"]]

    if not reachable:
        return None

    highest_term = max(node["term"] for node in reachable)

    leaders = [
        node for node in reachable
        if node["role"] == "leader"
        and node["term"] == highest_term
        and node["leader_id"] == node["node_id"]
    ]

    if len(leaders) != 1:
        return None

    leader = leaders[0]

    confirmations = {
        node["node_id"]
        for node in reachable
        if node["term"] == leader["term"] and node["leader_id"] == leader["node_id"]
    }

    return leader["node_id"] if len(confirmations) >= 2 else None


class ClusterMonitor:
    def __init__(self, addresses, query=query_node):
        self.addresses = list(addresses)
        self.query = query
        self.events = deque(maxlen=MAX_EVENTS)
        self.previous = None
        # Remember each address's node id so a down node is still labelled
        # "node2" rather than by its address.
        self.known_ids = {}
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=max(1, len(self.addresses)))

    def snapshot(self):
        nodes = list(self.executor.map(self.query, self.addresses))

        for node in nodes:
            if node["reachable"]:
                self.known_ids[node["address"]] = node["node_id"]
            elif node["address"] in self.known_ids:
                node["node_id"] = self.known_ids[node["address"]]

        # Keep nodes in a stable order: by node id when known, else address.
        nodes.sort(key=lambda node: (node.get("node_id") or 999, node["address"]))

        leader_id = agreed_leader(nodes)
        reachable = [node for node in nodes if node["reachable"]]
        highest_term = max((node["term"] for node in reachable), default=0)

        quorum = len(self.addresses) // 2 + 1
        healthy = len(reachable) >= quorum and leader_id is not None

        with self.lock:
            self._record_events(nodes, leader_id, highest_term)
            events = list(self.events)
            self.previous = {
                "leader_id": leader_id,
                "term": highest_term,
                "reachable": {node["address"]: node["reachable"] for node in nodes},
            }

        return {
            "timestamp": time.time(),
            "nodes": nodes,
            "leader_id": leader_id,
            "term": highest_term,
            "reachable_count": len(reachable),
            "total_count": len(self.addresses),
            "quorum": quorum,
            "healthy": healthy,
            "events": events,
        }

    def _record_events(self, nodes, leader_id, term):
        previous = self.previous
        now = time.time()

        def add(kind, message):
            self.events.appendleft({"time": now, "kind": kind, "message": message})

        if previous is None:
            if leader_id is not None:
                add("leader", f"node{leader_id} is leader in term {term}")
            return

        for node in nodes:
            was_up = previous["reachable"].get(node["address"])
            label = f"node{node['node_id']}" if node.get("node_id") else node["address"]

            if was_up and not node["reachable"]:
                add("down", f"{label} stopped responding")

            elif was_up is False and node["reachable"]:
                add("up", f"{label} is back as {node['role']} in term {node['term']}")

        if term > previous["term"]:
            add("term", f"term advanced {previous['term']} → {term} (election)")

        if leader_id != previous["leader_id"]:
            if leader_id is None:
                add("election", "no agreed leader, election in progress")

            else:
                add("leader", f"node{leader_id} elected leader in term {term}")
