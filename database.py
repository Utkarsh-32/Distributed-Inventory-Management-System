import hashlib
import hmac
import secrets
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


DB_PATH = Path(__file__).resolve().parent / "inventory.db"


# ============================================================
# DATABASE CONNECTION
# ============================================================

def get_connection():
    conn = sqlite3.connect(
        DB_PATH,
        timeout=15,
    )

    conn.row_factory = sqlite3.Row

    # Tell SQLite to enforce foreign-key relationships.
    conn.execute("PRAGMA foreign_keys = ON")

    return conn


# ============================================================
# TIME
# ============================================================

def utc_now():
    return datetime.now(timezone.utc).isoformat()


# ============================================================
# PASSWORD VERIFICATION
# ============================================================

def verify_password(password: str, stored_hash: str) -> bool:
    """
    Verify a password against the scrypt-based hash created
    by seed_db.py.

    Stored format:

        scrypt$n=16384$r=8$p=1$<salt>$<derived_key>
    """

    try:
        algorithm, n_part, r_part, p_part, salt_hex, key_hex = (
            stored_hash.split("$")
        )

        if algorithm != "scrypt":
            return False

        n = int(n_part.split("=")[1])
        r = int(r_part.split("=")[1])
        p = int(p_part.split("=")[1])

        salt = bytes.fromhex(salt_hex)
        expected_key = bytes.fromhex(key_hex)

        actual_key = hashlib.scrypt(
            password.encode("utf-8"),
            salt=salt,
            n=n,
            r=r,
            p=p,
            dklen=len(expected_key),
        )

        return hmac.compare_digest(
            actual_key,
            expected_key,
        )

    except (ValueError, IndexError):
        return False


# ============================================================
# DATABASE INITIALIZATION
# ============================================================

def initialize_database():
    conn = get_connection()

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sessions (
            token_hash TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT,
            FOREIGN KEY (user_id)
                REFERENCES users(id)
                ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS products (
            item_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            stock INTEGER NOT NULL CHECK (stock >= 0)
        );

        CREATE TABLE IF NOT EXISTS sales_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id TEXT NOT NULL,
            day_index INTEGER NOT NULL,
            units_sold INTEGER NOT NULL CHECK (units_sold >= 0),
            FOREIGN KEY (item_id)
                REFERENCES products(item_id)
                ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            item_id TEXT NOT NULL,
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id)
                REFERENCES users(id),
            FOREIGN KEY (item_id)
                REFERENCES products(item_id)
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_user_id
            ON sessions(user_id);

        CREATE INDEX IF NOT EXISTS idx_orders_user_id
            ON orders(user_id);

        CREATE INDEX IF NOT EXISTS idx_orders_item_id
            ON orders(item_id);

        CREATE INDEX IF NOT EXISTS idx_sales_history_item_id
            ON sales_history(item_id);
        """
    )

    conn.commit()
    conn.close()


# ============================================================
# AUTHENTICATION
# ============================================================

def authenticate_user(username: str, password: str):
    """
    Check username/password.

    If valid:
        create a random session token
        store only its SHA-256 hash
        return the original token

    If invalid:
        return None
    """

    conn = get_connection()

    try:
        row = conn.execute(
            """
            SELECT id, password_hash
            FROM users
            WHERE username = ?
            """,
            (username,),
        ).fetchone()

        if row is None:
            return None

        if not verify_password(
            password,
            row["password_hash"],
        ):
            return None

        # Generate a cryptographically random session token.
        token = secrets.token_urlsafe(32)

        # We store only the hash of the token.
        token_hash = hashlib.sha256(
            token.encode("utf-8")
        ).hexdigest()

        conn.execute(
            """
            INSERT INTO sessions
                (token_hash, user_id, created_at)
            VALUES
                (?, ?, ?)
            """,
            (
                token_hash,
                row["id"],
                utc_now(),
            ),
        )

        conn.commit()

        return token

    finally:
        conn.close()


def get_user_id_from_token(token: str):
    """
    Return the user ID associated with a valid session token.

    Return None if the token is invalid.
    """

    token_hash = hashlib.sha256(
        token.encode("utf-8")
    ).hexdigest()

    conn = get_connection()

    try:
        row = conn.execute(
            """
            SELECT user_id
            FROM sessions
            WHERE token_hash = ?
            """,
            (token_hash,),
        ).fetchone()

        if row is None:
            return None

        return row["user_id"]

    finally:
        conn.close()


def logout_user(token: str):
    """
    Delete the session associated with the token.
    """

    token_hash = hashlib.sha256(
        token.encode("utf-8")
    ).hexdigest()

    conn = get_connection()

    try:
        cursor = conn.execute(
            """
            DELETE FROM sessions
            WHERE token_hash = ?
            """,
            (token_hash,),
        )

        conn.commit()

        return cursor.rowcount > 0

    finally:
        conn.close()


# ============================================================
# INVENTORY
# ============================================================

def get_all_products():
    """
    Return the current inventory.
    """

    conn = get_connection()

    try:
        rows = conn.execute(
            """
            SELECT item_id, name, stock
            FROM products
            ORDER BY item_id
            """
        ).fetchall()

        return [dict(row) for row in rows]

    finally:
        conn.close()


def place_order(user_id: int, item_id: str, quantity: int):
    """
    Atomically place an order.

    Returns a dictionary describing the result.
    """

    # Validate before touching the database.
    if quantity <= 0:
        return {
            "success": False,
            "status": "INVALID_QUANTITY",
            "message": "Quantity must be greater than zero.",
        }

    conn = get_connection()

    try:
        # BEGIN IMMEDIATE obtains a write transaction.
        #
        # This is important for concurrency:
        # only one writer can perform the stock-changing
        # transaction at a time.
        conn.execute("BEGIN IMMEDIATE")

        product = conn.execute(
            """
            SELECT item_id, name, stock
            FROM products
            WHERE item_id = ?
            """,
            (item_id,),
        ).fetchone()

        if product is None:
            conn.rollback()

            return {
                "success": False,
                "status": "ITEM_NOT_FOUND",
                "message": "The requested product does not exist.",
            }

        current_stock = product["stock"]

        # Critical inventory check.
        if current_stock < quantity:
            conn.rollback()

            return {
                "success": False,
                "status": "FAILED_INSUFFICIENT_STOCK",
                "message": (
                    f"Only {current_stock} units of "
                    f"{product['name']} are available."
                ),
            }

        # Decrease the stock.
        conn.execute(
            """
            UPDATE products
            SET stock = stock - ?
            WHERE item_id = ?
            """,
            (quantity, item_id),
        )

        remaining_stock = current_stock - quantity

        # Record the successful order.
        cursor = conn.execute(
            """
            INSERT INTO orders
                (user_id, item_id, quantity, status, created_at)
            VALUES
                (?, ?, ?, ?, ?)
            """,
            (
                user_id,
                item_id,
                quantity,
                "SUCCESS",
                utc_now(),
            ),
        )

        order_id = cursor.lastrowid

        # Everything succeeded.
        conn.commit()

        return {
            "success": True,
            "status": "SUCCESS",
            "message": (
                f"Order #{order_id} placed successfully. "
                f"Ordered {quantity} × {product['name']}. "
                f"Remaining stock: {remaining_stock}."
            ),
            "order_id": order_id,
            "remaining_stock": remaining_stock,
        }

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    initialize_database()
    print(f"Database initialized at: {DB_PATH}")