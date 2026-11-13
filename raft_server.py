import raft_pb2, raft_pb2_grpc
import grpc
from concurrent import futures
from raft_node import RaftNode
import argparse
import threading
import json
from raft_replication import RaftLogStore
import database

class RaftServicer(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node, app_address):
        self.node = node
        self.app_address = app_address

        self.log_store = RaftLogStore()
        self.node.log_store = self.log_store

        db_state = self.log_store.get_state()

        db_term = db_state["current_term"]
        db_vote = db_state["voted_for"]

        if db_vote is not None:
            db_vote = int(db_vote)

        node_term = self.node.current_term
        node_vote = self.node.voted_for

        if db_term > node_term:

            self.node.current_term = db_term
            self.node.voted_for = db_vote
            self.node.role = "follower"
            self.node.leader_id = None

            self.node.save_state()

        elif node_term > db_term:

            self.node.save_state()

        else:

            if (
                db_vote is not None
                and node_vote is not None
                and db_vote != node_vote
            ):
                raise RuntimeError(
                    "Conflicting persisted votes for the same Raft term."
                )

            if node_vote is None and db_vote is not None:
                self.node.voted_for = db_vote

            if self.node.voted_for != db_vote:
                self.node.save_state()

        last_info = self.log_store.get_last_log_info()

        self.node.last_log_index = last_info["last_log_index"]
        self.node.last_log_term = last_info["last_log_term"]

        final_state = self.log_store.get_state()

        if final_state["commit_index"] > last_info["last_log_index"]:
            raise RuntimeError(
                "Commit index exceeds the available Raft log."
            )

        if final_state["last_applied"] > final_state["commit_index"]:
            raise RuntimeError(
                "Last applied index exceeds commit index."
            )


    def RequestVote(self, request, context):
        with self.node.lock:
            granted = self.node.request_vote(
                candidate_id=request.candidate_id,
                candidate_term=request.term,
                candidate_last_term=request.last_log_term,
                candidate_last_index=request.last_log_index
            )
            return raft_pb2.VoteResponse(term=self.node.current_term, vote_granted=granted)

    def Heartbeat(self, request, context):
        with self.node.lock:
            accepted = self.node.receive_heartbeat(request.leader_id, request.term)
            return raft_pb2.HeartbeatResponse(term=self.node.current_term, accepted=accepted)

    def AppendEntries(self, request, context):

        with self.node.lock:

            saved_state = self.log_store.get_state()

            if saved_state["current_term"] > self.node.current_term:
                self.node.become_follower(
                    saved_state["current_term"]
                )

            current_term = self.node.current_term

            if request.term < current_term:
                return raft_pb2.AppendEntriesResponse(
                    term=current_term,
                    success=False,
                    reason="STALE_TERM"
                )

            accepted = self.node.receive_heartbeat(
                request.leader_id,
                request.term
            )

            if not accepted:
                return raft_pb2.AppendEntriesResponse(
                    term=self.node.current_term,
                    success=False,
                    reason="STALE_TERM"
                )

            entries = []

            try:
                for item in request.entries:

                    command = json.loads(item.command_json)

                    if not isinstance(command, dict):
                        raise ValueError(
                            "Log command must be a JSON object."
                        )

                    entries.append({
                        "index": item.index,
                        "term": item.term,
                        "command_type": item.command_type,
                        "command": command,
                        "request_id": item.request_id or None,
                        "created_at": item.created_at
                    })

            except (ValueError, TypeError) as exc:

                return raft_pb2.AppendEntriesResponse(
                    term=self.node.current_term,
                    success=False,
                    reason=f"INVALID_ENTRY: {exc}"
                )

            try:

                result = self.log_store.append_entries(
                    term=request.term,
                    prev_log_index=request.prev_log_index,
                    prev_log_term=request.prev_log_term,
                    entries=entries,
                    leader_commit=request.leader_commit
                )

            except (ValueError, RuntimeError, TypeError) as exc:

                return raft_pb2.AppendEntriesResponse(
                    term=self.node.current_term,
                    success=False,
                    reason=str(exc)
                )

            if result["success"]:
                try:
                    self.log_store.apply_committed_entries()

                except Exception as exc:
                    print(
                        f"Node {self.node.node_id}: "
                        f"Committed entry application failed: {exc}",
                        flush=True
                    )

            last_info = self.log_store.get_last_log_info()

            self.node.last_log_index = last_info["last_log_index"]
            self.node.last_log_term = last_info["last_log_term"]

            return raft_pb2.AppendEntriesResponse(
                term=self.node.current_term,
                success=result["success"],
                match_index=result.get("match_index", 0),
                conflict_index=result.get("conflict_index", 0),
                reason=result.get("reason", "")
            )

    def SubmitOrder(self, request, context):

        user_id = request.user_id
        item_id = request.item_id.strip()
        quantity = request.quantity
        idempotency_key = request.idempotency_key.strip()

        def response(
            success,
            status,
            message,
            order_id=0,
            log_index=0,
            term=0,
            committed=False
        ):
            return raft_pb2.SubmitOrderResponse(
                success=success,
                status=status,
                message=message,
                order_id=order_id,
                log_index=log_index,
                term=term,
                committed=committed
            )

        if user_id <= 0 or not item_id or quantity <= 0 or not idempotency_key:
            return response(
                False,
                "INVALID_REQUEST",
                "Invalid order details."
            )

        with self.node.lock:

            if self.node.role != "leader":
                return response(
                    False,
                    "NOT_LEADER",
                    "This node is not the current Raft leader.",
                    term=self.node.current_term
                )

            current_term = self.node.current_term

        request_id = f"order:{user_id}:{idempotency_key}"

        command = {
            "user_id": user_id,
            "item_id": item_id,
            "quantity": quantity,
            "idempotency_key": idempotency_key
        }

        try:
            entry = self.log_store.append_local_entry(
                term=current_term,
                command_type="PLACE_ORDER",
                command=command,
                request_id=request_id
            )

            replication_result = self.node.replicate_logs()

        except Exception as exc:
            print(f"[RAFT ORDER] Replication error: {exc}", flush=True)

            return response(
                False,
                "REPLICATION_ERROR",
                "Unable to replicate the order.",
                term=current_term
            )

        state = self.log_store.get_state()

        if state["commit_index"] < entry["index"]:
            return response(
                False,
                "NO_QUORUM",
                "Order is not committed because a majority was not reached. Retry using the same idempotency key.",
                log_index=entry["index"],
                term=current_term,
                committed=False
            )

        try:
            self.log_store.apply_committed_entries()

        except Exception as exc:
            print(f"[RAFT ORDER] Application error: {exc}", flush=True)

            return response(
                False,
                "COMMITTED_APPLY_PENDING",
                "Order is committed but inventory application is pending.",
                log_index=entry["index"],
                term=current_term,
                committed=True
            )

        conn = database.get_connection()

        try:
            order = conn.execute(
                """
                SELECT id, status
                FROM orders
                WHERE user_id = ?
                AND idempotency_key = ?
                """,
                (user_id, request_id)
            ).fetchone()

        finally:
            conn.close()

        if order is None:
            return response(
                False,
                "ORDER_RESULT_MISSING",
                "Committed order result could not be found.",
                log_index=entry["index"],
                term=current_term,
                committed=True
            )

        order_status = order["status"]

        return response(
            order_status == "SUCCESS",
            order_status,
            f"Order #{order['id']}: {order_status}",
            order_id=order["id"],
            log_index=entry["index"],
            term=current_term,
            committed=True
        )


    def GetStatus(self, request, context):
        with self.node.lock:
            response = raft_pb2.StatusResponse(
                node_id=self.node.node_id,
                role=self.node.role,
                term=self.node.current_term,
                app_address=self.app_address
            )
            if self.node.leader_id is not None:
                response.leader_id = self.node.leader_id
            return response


def serve(node_id, address, peers, app_address):
    node = RaftNode(node_id=node_id, address=address, peers=peers)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    servicer = RaftServicer(node, app_address)
    raft_pb2_grpc.add_RaftServiceServicer_to_server(servicer=servicer, server=server)
    bound_port = server.add_insecure_port(node.address)

    print("gRPC bind result:", bound_port, flush=True)

    if bound_port == 0:
        raise RuntimeError(
            f"Failed to bind gRPC server to {node.address}"
        )

    server.start()

    print(
        f"Node {node.node_id} gRPC server listening on {node.address}",
        flush=True
    )

    timer_thread = threading.Thread(
        target=node.run_timer,
        daemon=True
    )

    timer_thread.start()
    server.wait_for_termination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-id", type=int, required=True)
    parser.add_argument("--address", required=True)
    parser.add_argument("--peers", nargs="+", required=True)
    parser.add_argument("--app-address", type=str, required=True)
    args = parser.parse_args()
    serve(args.node_id, args.address, args.peers, args.app_address)
