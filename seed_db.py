import argparse
import hashlib
import secrets
from datetime import datetime, timezone

import database
from mock_data import PRODUCTS, SUPPLIERS, WAREHOUSES, get_all_history_rows


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def hash_password(password: str) -> str:
    """
    Hash a password using scrypt with a random salt.

    We store:
        algorithm + parameters + salt + derived key

    instead of storing the actual password.
    """
    salt = secrets.token_bytes(16)

    derived_key = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=2**14,
        r=8,
        p=1,
        dklen=32,
    )

    return (
        "scrypt"
        "$n=16384"
        "$r=8"
        "$p=1"
        f"${salt.hex()}"
        f"${derived_key.hex()}"
    )


def seed_database():
    database.initialize_database()

    conn = database.get_connection()

    try:
        # Start with a clean demo state.
        conn.execute("BEGIN")

        # Delete dependent rows first because of foreign keys.
        conn.execute("DELETE FROM delivery_events")
        conn.execute("DELETE FROM order_allocations")
        conn.execute("DELETE FROM restock_events")
        conn.execute("DELETE FROM restock_requests")
        conn.execute("DELETE FROM warehouse_stock")
        conn.execute("DELETE FROM sessions")
        conn.execute("DELETE FROM orders")
        conn.execute("DELETE FROM sales_history")
        conn.execute("DELETE FROM products")
        conn.execute("DELETE FROM suppliers")
        conn.execute("DELETE FROM warehouses")
        conn.execute("DELETE FROM users")

        # --------------------------------------------------
        # 1. Demo users
        # --------------------------------------------------

        users = [
            ("alice", "password"),
            ("bob", "password"),
        ]

        for username, password in users:
            conn.execute(
                """
                INSERT INTO users
                    (username, password_hash, created_at)
                VALUES
                    (?, ?, ?)
                """,
                (
                    username,
                    hash_password(password),
                    utc_now(),
                ),
            )

        # --------------------------------------------------
        # 2. Products
        # --------------------------------------------------

        for product in PRODUCTS:
            conn.execute(
                """
                INSERT INTO products
                    (item_id, name, stock)
                VALUES
                    (?, ?, ?)
                """,
                (
                    product.item_id,
                    product.name,
                    product.initial_stock,
                ),
            )

        # --------------------------------------------------
        # 3. Historical sales
        # --------------------------------------------------

        conn.executemany(
            """
            INSERT INTO sales_history
                (item_id, day_index, units_sold)
            VALUES
                (?, ?, ?)
            """,
            get_all_history_rows(),
        )

        # --------------------------------------------------
        # 4. Warehouses, suppliers and per-warehouse stock
        # --------------------------------------------------

        database._ensure_reference_data(conn)
        database._ensure_warehouse_stock(conn)

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

    print("========================================")
    print("Database seeded successfully")
    print("========================================")
    print("Demo users:")
    print("  alice / password")
    print("  bob   / password")
    print()
    print(f"Products loaded: {len(PRODUCTS)}")
    print(
        "Historical observations:",
        len(get_all_history_rows())
    )
    print(f"Warehouses: {len(WAREHOUSES)}")
    print(f"Suppliers: {len(SUPPLIERS)}")
    print()
    print("Wireless Mouse initial stock: 20")
    print(f"Database: {database.DB_PATH}")
    print("========================================")


def database_has_users():
    database.initialize_database()

    conn = database.get_connection()

    try:
        return conn.execute("SELECT 1 FROM users LIMIT 1").fetchone() is not None
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reset the demo database and load demo data."
    )
    parser.add_argument(
        "--if-empty",
        action="store_true",
        help=(
            "Only seed when the database has no users yet. Used by the "
            "Docker and start scripts so restarts keep existing data."
        ),
    )
    args = parser.parse_args()

    if args.if_empty and database_has_users():
        print(f"Database already has data, skipping seed: {database.DB_PATH}")
    else:
        seed_database()