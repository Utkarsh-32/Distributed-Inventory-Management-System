"""Last-laptop demonstration using disposable instances of the project's Raft server."""
import copy
import hashlib
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

import grpc
import raft_pb2 as pb
import raft_pb2_grpc as rpc


class DemoLinks(grpc.ServerInterceptor):
    """Cut only peer-to-peer Raft messages. Clients can still query either side."""
    def __init__(self, node_id, cut_file):
        self.node_id = node_id
        self.cut_file = cut_file

    def intercept_service(self, continuation, details):
        handler = continuation(details)
        method = details.method.rsplit("/", 1)[-1]
        if handler is None or method not in {"RequestVote", "Heartbeat", "AppendEntries"}:
            return handler

        def deliver(request, context):
            sender = request.candidate_id if method == "RequestVote" else request.leader_id
            try:
                isolated = int(self.cut_file.read_text())
            except FileNotFoundError:
                isolated = 0
            if isolated in (sender, self.node_id):
                context.abort(grpc.StatusCode.UNAVAILABLE, "Demo link disconnected")
            return handler.unary_unary(request, context)

        return grpc.unary_unary_rpc_method_handler(
            deliver, request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


def serve_demo_node(node_id, parent_pid, preferred_leader=0):
    import database
    from raft_node import RaftNode
    from raft_server import RaftServicer

    directory = Path.cwd()
    database.initialize_database()
    conn = database.get_connection()
    with conn:
        conn.execute("INSERT INTO users(id,username,password_hash,created_at) VALUES (1,'demo','','demo')")
        conn.execute("INSERT INTO products VALUES ('demo-laptop','Demo laptop',1)")
    conn.close()
    server = grpc.server(ThreadPoolExecutor(max_workers=4),
                         interceptors=[DemoLinks(node_id, directory / "cut")])
    port = server.add_insecure_port("127.0.0.1:0")
    address = f"127.0.0.1:{port}"
    ready = directory / f"node{node_id}.addr"
    ready.with_suffix(".tmp").write_text(address)
    ready.with_suffix(".tmp").replace(ready)
    while not all((directory / f"node{i}.addr").exists() for i in (1, 2, 3)):
        if os.getppid() != parent_pid:
            return
        time.sleep(.05)
    peers = [(directory / f"node{i}.addr").read_text() for i in (1, 2, 3) if i != node_id]
    node = RaftNode(node_id, address, peers)
    if preferred_leader and node_id != preferred_leader:
        # Give the preferred node the first election so the demo leader
        # matches the live cluster's leader.  If it cannot win, the other
        # nodes time out normally and elect one of themselves.
        with node.lock:
            node.election_deadline = time.monotonic() + 12
    rpc.add_RaftServiceServicer_to_server(RaftServicer(node, ""), server)
    server.start()
    threading.Thread(target=node.run_timer, daemon=True).start()
    while os.getppid() == parent_pid:
        time.sleep(.5)
    server.stop(0)


class PartitionDemo:
    def __init__(self):
        self.lock = threading.Lock()
        self.reconnect_event = threading.Event()
        self.stop_event = threading.Event()
        self.state = {"phase": "idle", "nodes": [], "orders": {}, "isolated": None}
        self.preferred_leader = 0
        self.owner = None
        self.worker = None

    @staticmethod
    def owner_key(token):
        return hashlib.sha256(token.encode()).digest()

    def snapshot(self, token):
        with self.lock:
            if self.owner and self.owner != self.owner_key(token):
                return {"phase": "busy" if self.worker and self.worker.is_alive() else "idle",
                        "nodes": [], "orders": {}, "isolated": None}
            return copy.deepcopy(self.state)

    def start(self, token, preferred_leader=None):
        with self.lock:
            if self.worker and self.worker.is_alive():
                return False
            self.owner = self.owner_key(token)
            self.preferred_leader = preferred_leader if preferred_leader in (1, 2, 3) else 0
            self.reconnect_event.clear()
            self.stop_event.clear()
            self.state = {"phase": "starting", "nodes": [], "orders": {}, "isolated": None}
            self.worker = threading.Thread(target=self._run, daemon=True)
            self.worker.start()
            return True

    def reconnect(self, token):
        with self.lock:
            if self.owner != self.owner_key(token) or self.state["phase"] != "partitioned":
                return False
            self.reconnect_event.set()
            return True

    def close(self):
        self.stop_event.set()
        self.reconnect_event.set()
        if self.worker:
            self.worker.join(timeout=8)

    def update(self, **values):
        with self.lock:
            self.state.update(values)

    def _run(self):
        processes = []
        channels = []
        try:
            with tempfile.TemporaryDirectory(prefix="inventory-partition-") as work:
                directory = Path(work)
                try:
                    for i in (1, 2, 3):
                        processes.append(subprocess.Popen(
                            [sys.executable, str(Path(__file__).resolve()), str(i), str(os.getpid()),
                             str(self.preferred_leader)],
                            cwd=directory, env={**os.environ, "INVENTORY_DB_PATH": str(directory / f"{i}.db")},
                            stdout=subprocess.DEVNULL,
                        ))
                    def ready():
                        return all((directory / f"node{i}.addr").exists() for i in (1, 2, 3))
                    self._wait(ready, processes)
                    for i in (1, 2, 3):
                        channels.append(grpc.insecure_channel((directory / f"node{i}.addr").read_text()))
                    self._exercise(directory, [rpc.RaftServiceStub(c) for c in channels], processes)
                finally:
                    for channel in channels:
                        channel.close()
                    for process in processes:
                        if process.poll() is None:
                            process.terminate()
                    for process in processes:
                        try:
                            process.wait(timeout=2)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
        except Exception as exc:
            self.update(phase="error", message=str(exc))

    def _wait(self, probe, processes, seconds=25):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if self.stop_event.is_set() or any(p.poll() is not None for p in processes):
                raise RuntimeError("The demo stopped before verification.")
            result = probe()
            if result:
                return result
            time.sleep(.15)
        raise RuntimeError("The demo timed out waiting for the nodes to agree. Start a fresh run.")

    def _exercise(self, directory, stubs, processes):
        def observe():
            nodes = []
            for i, stub in enumerate(stubs, 1):
                try:
                    status = stub.GetStatus(pb.StatusRequest(), timeout=.5)
                except grpc.RpcError:
                    return []
                with closing(sqlite3.connect(f"file:{directory}/{i}.db?mode=ro", uri=True)) as db:
                    db.execute("BEGIN")
                    stock = db.execute("SELECT stock FROM products WHERE item_id='demo-laptop'").fetchone()[0]
                    sales = [r[0] for r in db.execute("SELECT idempotency_key FROM orders WHERE status='SUCCESS'")]
                    log = db.execute(
                        "SELECT request_id, term, command_type, command_json FROM raft_log ORDER BY log_index"
                    ).fetchall()
                    entries = [r[0] for r in log]
                nodes.append(dict(id=i, role=status.role, term=status.term, leader=status.leader_id,
                                  stock=stock, sales=sales, entries=entries, log=log, committed=status.commit_index,
                                  applied=status.last_applied))
            self.update(nodes=nodes)
            return nodes

        def agreed(excluded=None):
            nodes = observe()
            for n in nodes:
                if n["id"] != excluded and n["role"] == "leader" and all(
                    p["leader"] == n["id"] and p["term"] == n["term"]
                    for p in nodes if p["id"] != excluded
                ):
                    return n["id"]
            return None

        orders = {}
        def purchase(node_id, side):
            orders[side] = {"node": node_id, "status": "Submitted"}
            self.update(orders=copy.deepcopy(orders))
            request = pb.SubmitOrderRequest(user_id=1, item_id="demo-laptop", quantity=1, idempotency_key=side)
            result = stubs[node_id-1].SubmitOrder(request, timeout=5)
            orders[side].update(status=result.status, index=result.log_index, committed=result.committed,
                                term=result.term)
            self.update(orders=copy.deepcopy(orders))
            return result

        original = self._wait(agreed, processes)
        cut = directory / "cut"
        temp = directory / "cut.tmp"
        temp.write_text(str(original))
        temp.replace(cut)
        self.update(phase="electing", isolated=original)
        denied = purchase(original, "minority")
        if denied.committed or denied.status != "NO_QUORUM":
            raise RuntimeError("The isolated node did not return the expected no-majority result.")
        winner = self._wait(lambda: agreed(original), processes)
        def buy_on_majority():
            result = purchase(winner, "majority")
            if result.status == "NO_QUORUM":
                return False
            if not result.success or not result.committed:
                raise RuntimeError(f"Majority purchase returned {result.status}.")
            return True
        self._wait(buy_on_majority, processes)
        self._wait(lambda: sum(n["sales"] == ["order:1:majority"] for n in observe()) == 2, processes)
        self.update(phase="partitioned")
        # Restore automatically if the user leaves the page.
        deadline = time.monotonic() + 120
        while not self.reconnect_event.wait(.6) and time.monotonic() < deadline:
            observe()
        cut.unlink()
        self.update(phase="recovering", isolated=None)

        def converged():
            nodes = observe()
            return len(nodes) == 3 and sum(n["role"] == "leader" for n in nodes) == 1 and all(
                n["stock"] == 0 and n["sales"] == ["order:1:majority"]
                and n["entries"] == ["order:1:majority"] and n["log"] == nodes[0]["log"]
                and n["applied"] == n["committed"] == 1
                and n["term"] == nodes[0]["term"] and n["leader"] == winner for n in nodes
            )
        self._wait(converged, processes)
        self.update(phase="complete")


if __name__ == "__main__":
    serve_demo_node(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]) if len(sys.argv) > 3 else 0)
