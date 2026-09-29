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

    _ensure_sales_history_tracking_column(conn)
    _record_legacy_orders_in_sales_history(conn)

    conn.commit()
    conn.close()


def _ensure_sales_history_tracking_column(conn):
    """Add the order-to-sales-history marker for existing databases."""
    columns = conn.execute(
        "PRAGMA table_info(orders)"
    ).fetchall()

    if any(column["name"] == "sales_history_recorded" for column in columns):
        return

    conn.execute(
        """
        ALTER TABLE orders
        ADD COLUMN sales_history_recorded INTEGER NOT NULL DEFAULT 0
        """
    )


def _record_sale_in_latest_history(conn, item_id, quantity):
    """Add a sale to the latest daily observation for one product."""
    latest_history = conn.execute(
        """
        SELECT id
        FROM sales_history
        WHERE item_id = ?
        ORDER BY day_index DESC, id DESC
        LIMIT 1
        """,
        (item_id,),
    ).fetchone()

    if latest_history is None:
        conn.execute(
            """
            INSERT INTO sales_history
                (item_id, day_index, units_sold)
            VALUES
                (?, ?, ?)
            """,
            (item_id, 1, quantity),
        )
        return

    conn.execute(
        """
        UPDATE sales_history
        SET units_sold = units_sold + ?
        WHERE id = ?
        """,
        (quantity, latest_history["id"]),
    )


def _record_legacy_orders_in_sales_history(conn):
    """Bring orders created before sales tracking into forecast history."""
    legacy_orders = conn.execute(
        """
        SELECT item_id, SUM(quantity) AS total_quantity
        FROM orders
        WHERE status = 'SUCCESS'
          AND sales_history_recorded = 0
        GROUP BY item_id
        """
    ).fetchall()

    for order in legacy_orders:
        _record_sale_in_latest_history(
            conn,
            order["item_id"],
            int(order["total_quantity"]),
        )

    if legacy_orders:
        conn.execute(
            """
            UPDATE orders
            SET sales_history_recorded = 1
            WHERE status = 'SUCCESS'
              AND sales_history_recorded = 0
            """
        )


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

        # A successful order is also a sale.  Include it in the most recent
        # daily observation so the rolling seven-day average, trend, and
        # forecast all reflect the same transaction as the stock level.
        _record_sale_in_latest_history(
            conn,
            item_id,
            quantity,
        )

        # Record the successful order.
        cursor = conn.execute(
            """
            INSERT INTO orders
                (
                    user_id,
                    item_id,
                    quantity,
                    status,
                    created_at,
                    sales_history_recorded
                )
            VALUES
                (?, ?, ?, ?, ?, 1)
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


def get_order_history(user_id: int):
    """
    Return the most recent successful orders for a user.
    """

    conn = get_connection()

    try:
        rows = conn.execute(
            """
            SELECT
                orders.id,
                orders.item_id,
                products.name AS product_name,
                orders.quantity,
                orders.status,
                orders.created_at
            FROM orders
            JOIN products
                ON products.item_id = orders.item_id
            WHERE orders.user_id = ?
            ORDER BY orders.id DESC
            LIMIT 20
            """,
            (user_id,),
        ).fetchall()

        return [dict(row) for row in rows]

    finally:
        conn.close()


def get_products_with_history():
    """
    Return every product together with its complete sales history.

    This is read-only. It does not modify inventory.
    """
    conn = get_connection()

    try:
        product_rows = conn.execute(
            """
            SELECT item_id, name, stock
            FROM products
            ORDER BY item_id
            """
        ).fetchall()

        history_rows = conn.execute(
            """
            SELECT item_id, day_index, units_sold
            FROM sales_history
            ORDER BY item_id, day_index
            """
        ).fetchall()

    finally:
        conn.close()

    history_by_item = {}

    for row in history_rows:
        history_by_item.setdefault(row["item_id"], []).append(
            int(row["units_sold"])
        )

    products = []

    for row in product_rows:
        item_id = row["item_id"]

        products.append(
            {
                "item_id": item_id,
                "product_name": row["name"],
                "current_stock": int(row["stock"]),
                "sales_history": history_by_item.get(item_id, []),
            }
        )

    return products


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    initialize_database()
    print(f"Database initialized at: {DB_PATH}")
