import json
import database

class RaftLogStore:
    """
    Handles persistent Raft log storage and metadata.

    Each node uses its own SQLite database.
    """

    def __init__(self):
        database.initialize_database()
        self.next_index = {}
        self.match_index = {}

    def get_state(self):
        conn = database.get_connection()

        try:
            row = conn.execute(
                """
                SELECT current_term, voted_for,
                       commit_index, last_applied
                FROM raft_metadata
                WHERE singleton_id = 1
                """
            ).fetchone()

            return dict(row)

        finally:
            conn.close()

    def get_last_log_info(self):
        conn = database.get_connection()

        try:
            row = conn.execute(
                """
                SELECT log_index, term
                FROM raft_log
                ORDER BY log_index DESC
                LIMIT 1
                """
            ).fetchone()

            if row is None:
                return {
                    "last_log_index": 0,
                    "last_log_term": 0
                }

            return {
                "last_log_index": row["log_index"],
                "last_log_term": row["term"]
            }

        finally:
            conn.close()

    def get_log_term(self, index):

        if index == 0:
            return 0

        conn = database.get_connection()

        try:
            row = conn.execute(
                """
                SELECT term
                FROM raft_log
                WHERE log_index = ?
                """,
                (index,)
            ).fetchone()

            return row["term"] if row else None

        finally:
            conn.close()

    def _save_higher_term(self, term):

        conn = database.get_connection()

        try:
            conn.execute(
                """
                UPDATE raft_metadata
                SET current_term = ?,
                    voted_for = NULL
                WHERE singleton_id = 1
                AND current_term < ?
                """,
                (term, term)
            )

            conn.commit()

        finally:
            conn.close()

    def append_local_entry(
        self,
        term,
        command_type,
        command,
        request_id=None
    ):

        if not isinstance(term, int) or term < 0:
            raise ValueError("Invalid Raft term.")

        if not command_type:
            raise ValueError("Command type is required.")

        if not isinstance(command, dict):
            raise TypeError("Command must be a dictionary.")

        if request_id is not None:
            request_id = str(request_id).strip()

            if not request_id:
                raise ValueError("Request ID cannot be empty.")

        command_json = json.dumps(
            command,
            sort_keys=True,
            separators=(",", ":")
        )

        conn = database.get_connection()

        try:
            conn.execute("BEGIN IMMEDIATE")

            state = conn.execute(
                """
                SELECT current_term
                FROM raft_metadata
                WHERE singleton_id = 1
                """
            ).fetchone()

            current_term = state["current_term"]

            if term < current_term:
                raise ValueError(
                    "Cannot append an entry from an older term."
                )
            if term > current_term:
                conn.execute(
                    """
                    UPDATE raft_metadata
                    SET current_term = ?,
                        voted_for = NULL
                    WHERE singleton_id = 1
                    """,
                    (term,)
                )

            if request_id is not None:

                existing = conn.execute(
                    """
                    SELECT *
                    FROM raft_log
                    WHERE request_id = ?
                    """,
                    (request_id,)
                ).fetchone()

                if existing is not None:

                    if (
                        existing["command_type"] != command_type
                        or existing["command_json"] != command_json
                    ):
                        raise ValueError(
                            "Request ID already exists "
                            "with a different command."
                        )

                    conn.commit()

                    return self._entry_from_row(existing)

            last_index = conn.execute(
                """
                SELECT COALESCE(MAX(log_index), 0)
                FROM raft_log
                """
            ).fetchone()[0]

            next_index = last_index + 1

            cursor = conn.execute(
                """
                INSERT INTO raft_log (
                    log_index,
                    term,
                    command_type,
                    command_json,
                    request_id,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    next_index,
                    term,
                    command_type,
                    command_json,
                    request_id,
                    database.utc_now()
                )
            )

            conn.commit()

            return {
                "index": cursor.lastrowid,
                "term": term,
                "command_type": command_type,
                "command": command,
                "request_id": request_id
            }

        except Exception:
            conn.rollback()
            raise

        finally:
            conn.close()

    def get_entries(self, start_index=1):

        conn = database.get_connection()

        try:
            rows = conn.execute(
                """
                SELECT *
                FROM raft_log
                WHERE log_index >= ?
                ORDER BY log_index ASC
                """,
                (start_index,)
            ).fetchall()

            return [
                self._entry_from_row(row)
                for row in rows
            ]

        finally:
            conn.close()

    def append_entries(
        self,
        term,
        prev_log_index,
        prev_log_term,
        entries,
        leader_commit
    ):

        if not all(
            isinstance(value, int) and value >= 0
            for value in (
                term,
                prev_log_index,
                prev_log_term,
                leader_commit
            )
        ):
            raise ValueError("Invalid AppendEntries parameters.")

        if not isinstance(entries, list):
            raise TypeError("Entries must be a list.")

        conn = database.get_connection()

        try:
            conn.execute("BEGIN IMMEDIATE")

            state = conn.execute(
                """
                SELECT current_term, commit_index
                FROM raft_metadata
                WHERE singleton_id = 1
                """
            ).fetchone()

            current_term = state["current_term"]
            commit_index = state["commit_index"]

            if term < current_term:
                conn.commit()
                return {
                    "success": False,
                    "term": current_term,
                    "reason": "STALE_TERM"
                }

            if term > current_term:
                conn.execute(
                    """
                    UPDATE raft_metadata
                    SET current_term = ?,
                        voted_for = NULL
                    WHERE singleton_id = 1
                    """,
                    (term,)
                )

            if prev_log_index == 0:

                if prev_log_term != 0:
                    raise ValueError(
                        "Previous term must be zero for index zero."
                    )

            else:
                previous = conn.execute(
                    """
                    SELECT term
                    FROM raft_log
                    WHERE log_index = ?
                    """,
                    (prev_log_index,)
                ).fetchone()

                if previous is None:
                    last_index = conn.execute(
                        """
                        SELECT COALESCE(MAX(log_index), 0)
                        FROM raft_log
                        """
                    ).fetchone()[0]

                    conn.commit()

                    return {
                        "success": False,
                        "term": term,
                        "conflict_index": last_index + 1
                    }

                if previous["term"] != prev_log_term:

                    if prev_log_index <= commit_index:
                        raise RuntimeError(
                            "Conflict with an already committed entry."
                        )

                    conflicting_term = previous["term"]

                    first = conn.execute(
                        """
                        SELECT MIN(log_index)
                        FROM raft_log
                        WHERE term = ?
                        """,
                        (conflicting_term,)
                    ).fetchone()[0]

                    conflict_index = first or prev_log_index

                    conn.commit()

                    return {
                        "success": False,
                        "term": term,
                        "conflict_index": conflict_index
                    }

            expected_index = prev_log_index + 1
            match_index = prev_log_index

            for entry in entries:

                index = entry.get("index")
                entry_term = entry.get("term")
                command_type = entry.get("command_type")
                command = entry.get("command")
                request_id = entry.get("request_id")

                if index != expected_index:
                    raise ValueError("Log indexes must be consecutive.")

                if (
                    not isinstance(entry_term, int)
                    or entry_term < 0
                    or entry_term > term
                ):
                    raise ValueError("Invalid entry term.")

                if not command_type or not isinstance(command, dict):
                    raise ValueError("Invalid log command.")

                command_json = json.dumps(
                    command,
                    sort_keys=True,
                    separators=(",", ":")
                )

                existing = conn.execute(
                    """
                    SELECT *
                    FROM raft_log
                    WHERE log_index = ?
                    """,
                    (index,)
                ).fetchone()

                if existing is not None:

                    if existing["term"] != entry_term:

                        if index <= commit_index:
                            raise RuntimeError(
                                "Cannot replace a committed entry."
                            )
                        conn.execute(
                            """
                            DELETE FROM raft_log
                            WHERE log_index >= ?
                            """,
                            (index,)
                        )

                        existing = None

                    else:
                        if (
                            existing["command_type"] != command_type
                            or existing["command_json"] != command_json
                            or existing["request_id"] != request_id
                        ):
                            raise RuntimeError(
                                "Same index and term have different commands."
                            )

                if existing is None:

                    later_entry = conn.execute(
                        """
                        SELECT MIN(log_index)
                        FROM raft_log
                        WHERE log_index >= ?
                        """,
                        (index,)
                    ).fetchone()[0]

                    if later_entry is not None:

                        if index <= commit_index:
                            raise RuntimeError(
                                "Cannot replace committed log entries."
                            )

                        conn.execute(
                            """
                            DELETE FROM raft_log
                            WHERE log_index >= ?
                            """,
                            (index,)
                        )

                    conn.execute(
                        """
                        INSERT INTO raft_log (
                            log_index,
                            term,
                            command_type,
                            command_json,
                            request_id,
                            created_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            index,
                            entry_term,
                            command_type,
                            command_json,
                            request_id,
                            entry.get(
                                "created_at",
                                database.utc_now()
                            )
                        )
                    )

                match_index = index
                expected_index += 1

            new_commit_index = min(
                leader_commit,
                match_index
            )

            if new_commit_index > commit_index:
                conn.execute(
                    """
                    UPDATE raft_metadata
                    SET commit_index = ?
                    WHERE singleton_id = 1
                    """,
                    (new_commit_index,)
                )

            conn.commit()

            return {
                "success": True,
                "term": term,
                "match_index": match_index,
                "commit_index": max(
                    commit_index,
                    new_commit_index
                )
            }

        except Exception:
            conn.rollback()
            raise

        finally:
            conn.close()

    def replicate_to_followers(
        self,
        followers,
        max_retries=8
    ):
        """
        Send the leader's log entries to followers.

        followers:
            Dictionary mapping follower IDs to RPC callbacks.

        Each callback accepts one AppendEntries request
        dictionary and returns a response dictionary.
        """

        if not isinstance(followers, dict):
            raise TypeError("Followers must be a dictionary.")

        if not all(callable(rpc) for rpc in followers.values()):
            raise TypeError("Every follower must have an RPC callback.")

        if not isinstance(max_retries, int) or max_retries < 1:
            raise ValueError("max_retries must be positive.")

        state = self.get_state()
        current_term = state["current_term"]

        last_info = self.get_last_log_info()
        last_index = last_info["last_log_index"]

        acknowledged = []
        failed = {}

        for follower_id, append_entries_rpc in followers.items():
            next_idx = self.next_index.get(
                follower_id,
                last_index + 1
            )

            replicated = False

            for attempt in range(max_retries):

                prev_index = next_idx - 1

                prev_term = self.get_log_term(prev_index)

                if prev_term is None:
                    next_idx = max(1, next_idx - 1)
                    continue

                entries = self.get_entries(next_idx)

                request = {
                    "term": current_term,
                    "prev_log_index": prev_index,
                    "prev_log_term": prev_term,
                    "entries": entries,
                    "leader_commit": state["commit_index"]
                }

                try:
                    response = append_entries_rpc(request)

                except Exception as exc:
                    failed[follower_id] = str(exc)
                    break

                response_term = response.get(
                    "term",
                    current_term
                )

                if response_term > current_term:

                    self._save_higher_term(response_term)

                    return {
                        "leadership_valid": False,
                        "higher_term": response_term,
                        "acknowledged": acknowledged,
                        "failed": failed
                    }

                if response.get("success"):

                    expected_match = prev_index + len(entries)

                    match_idx = response.get("match_index")

                    if match_idx != expected_match:
                        failed[follower_id] = (
                            "Follower returned an unexpected match index."
                        )
                        break

                    self.match_index[follower_id] = match_idx
                    self.next_index[follower_id] = match_idx + 1

                    acknowledged.append(follower_id)

                    replicated = True
                    break

                conflict_index = response.get("conflict_index")

                if (
                    isinstance(conflict_index, int)
                    and 1 <= conflict_index < next_idx
                ):
                    next_idx = conflict_index
                else:
                    next_idx = max(1, next_idx - 1)

            if not replicated and follower_id not in failed:
                failed[follower_id] = (
                    "Replication failed after maximum retries."
                )

            if not replicated:
                self.next_index[follower_id] = next_idx

        return {
            "leadership_valid": True,
            "term": current_term,
            "leader_last_index": last_index,
            "acknowledged": acknowledged,
            "failed": failed,
            "match_index": dict(self.match_index),
            "next_index": dict(self.next_index)
        }

    def advance_commit_index(self, cluster_size):

        if not isinstance(cluster_size, int) or cluster_size < 1:
            raise ValueError("Invalid cluster size.")

        majority = cluster_size // 2 + 1

        state = self.get_state()

        current_term = state["current_term"]
        current_commit = state["commit_index"]

        last_index = self.get_last_log_info()["last_log_index"]
        replicated_indexes = [last_index]

        replicated_indexes.extend(
            self.match_index.values()
        )

        new_commit_index = current_commit

        for index in range(last_index, current_commit, -1):

            entry_term = self.get_log_term(index)
            if entry_term != current_term:
                continue

            replicated_count = sum(
                1 for match in replicated_indexes
                if match >= index
            )

            if replicated_count >= majority:
                new_commit_index = index
                break

        if new_commit_index == current_commit:

            return {
                "committed": False,
                "commit_index": current_commit,
                "message": "No new entry is ready for commitment."
            }

        conn = database.get_connection()

        try:
            conn.execute("BEGIN IMMEDIATE")

            latest_state = conn.execute(
                """
                SELECT current_term, commit_index
                FROM raft_metadata
                WHERE singleton_id = 1
                """
            ).fetchone()

            if latest_state["current_term"] != current_term:

                conn.rollback()

                return {
                    "committed": False,
                    "commit_index": latest_state["commit_index"],
                    "message": "Raft term changed. Leadership must be rechecked."
                }

            new_commit_index = max(
                new_commit_index,
                latest_state["commit_index"]
            )

            conn.execute(
                """
                UPDATE raft_metadata
                SET commit_index = ?
                WHERE singleton_id = 1
                """,
                (new_commit_index,)
            )

            conn.commit()

            return {
                "committed": True,
                "commit_index": new_commit_index,
                "majority_required": majority,
                "replicated_nodes": replicated_count,
                "message": "Log entry committed successfully."
            }

        except Exception:
            conn.rollback()
            raise

        finally:
            conn.close()

    def apply_committed_entries(self):

        applied_entries = []

        while True:

            conn = database.get_connection()

            try:
                conn.execute("BEGIN IMMEDIATE")

                state = conn.execute(
                    """
                    SELECT commit_index, last_applied
                    FROM raft_metadata
                    WHERE singleton_id = 1
                    """
                ).fetchone()

                commit_index = state["commit_index"]
                last_applied = state["last_applied"]

                if last_applied >= commit_index:
                    conn.commit()
                    break

                next_index = last_applied + 1

                row = conn.execute(
                    """
                    SELECT *
                    FROM raft_log
                    WHERE log_index = ?
                    """,
                    (next_index,)
                ).fetchone()

                if row is None:
                    raise RuntimeError(
                        f"Committed log entry {next_index} is missing."
                    )

                entry = self._entry_from_row(row)

                if entry["command_type"] != "PLACE_ORDER":
                    raise ValueError(
                        "Unsupported command: "
                        + entry["command_type"]
                    )

                command = entry["command"]

                user_id = command.get("user_id")
                item_id = command.get("item_id")
                quantity = command.get("quantity")
                request_id = entry["request_id"]

                if type(user_id) is not int or user_id <= 0:
                    raise ValueError("Invalid user ID.")

                if not isinstance(item_id, str) or not item_id:
                    raise ValueError("Invalid product ID.")

                if type(quantity) is not int or quantity <= 0:
                    raise ValueError("Invalid order quantity.")

                if not request_id:
                    raise ValueError("Missing request ID.")

                existing = conn.execute(
                    """
                    SELECT id, user_id, item_id, quantity, status
                    FROM orders
                    WHERE request_id = ?
                    """,
                    (request_id,)
                ).fetchone()

                if existing is not None:

                    if (
                        existing["user_id"] != user_id
                        or existing["item_id"] != item_id
                        or existing["quantity"] != quantity
                    ):
                        raise RuntimeError(
                            "Request ID conflicts with an existing order."
                        )

                    result_status = existing["status"]

                else:

                    product = conn.execute(
                        """
                        SELECT name, stock
                        FROM products
                        WHERE item_id = ?
                        """,
                        (item_id,)
                    ).fetchone()

                    if product is None:
                        raise RuntimeError(
                            f"Product {item_id} is missing on this node."
                        )

                    current_stock = product["stock"]

                    if current_stock >= quantity:

                        conn.execute(
                            """
                            UPDATE products
                            SET stock = stock - ?
                            WHERE item_id = ?
                            """,
                            (quantity, item_id)
                        )

                        database._record_sale_in_latest_history(
                            conn,
                            item_id,
                            quantity
                        )

                        result_status = "SUCCESS"
                        sales_recorded = 1

                    else:
                        result_status = "FAILED_INSUFFICIENT_STOCK"
                        sales_recorded = 0

                    conn.execute(
                        """
                        INSERT INTO orders (
                            user_id,
                            item_id,
                            quantity,
                            status,
                            created_at,
                            sales_history_recorded,
                            request_id
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            user_id,
                            item_id,
                            quantity,
                            result_status,
                            entry["created_at"] or database.utc_now(),
                            sales_recorded,
                            request_id
                        )
                    )

                conn.execute(
                    """
                    UPDATE raft_metadata
                    SET last_applied = ?
                    WHERE singleton_id = 1
                    """,
                    (next_index,)
                )

                conn.commit()

                applied_entries.append({
                    "index": next_index,
                    "request_id": request_id,
                    "status": result_status
                })

            except Exception:
                conn.rollback()
                raise

            finally:
                conn.close()

        return applied_entries

    def recover_pending_entries(self):

        state = self.get_state()

        commit_index = state["commit_index"]
        last_applied = state["last_applied"]

        if last_applied > commit_index:
            raise RuntimeError(
                "Last applied index exceeds commit index."
            )

        last_index = self.get_last_log_info()["last_log_index"]

        if commit_index > last_index:
            raise RuntimeError(
                "Committed entries are missing. "
                "Synchronize with the leader first."
            )

        entries = self.get_entries(last_applied + 1)

        expected_indexes = list(
            range(last_applied + 1, commit_index + 1)
        )

        actual_indexes = [
            entry["index"] for entry in entries
        ]

        if actual_indexes[:len(expected_indexes)] != expected_indexes:
            raise RuntimeError(
                "Committed log entries are incomplete."
            )
        recovered = self.apply_committed_entries()

        return {
            "commit_index": commit_index,
            "last_applied": self.get_state()["last_applied"],
            "recovered_entries": recovered
        }

    @staticmethod
    def _entry_from_row(row):

        return {
            "index": row["log_index"],
            "term": row["term"],
            "command_type": row["command_type"],
            "command": json.loads(row["command_json"]),
            "request_id": row["request_id"],
            "created_at": row["created_at"]
        }
