import hashlib
import hmac
import os
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path


# Each node keeps its own database.  Docker and the local multi-node start
# scripts point INVENTORY_DB_PATH at that node's persistent data directory.
DB_PATH = Path(
    os.getenv(
        "INVENTORY_DB_PATH",
        str(Path(__file__).resolve().parent / "inventory.db"),
    )
)

# A warehouse holding this many units of a product or fewer is "low stock".
LOW_STOCK_THRESHOLD = 5


# ============================================================
# ORDER DELIVERY AND SUPPLIER RESTOCK WORKFLOWS
# ============================================================

DELIVERY_STATUSES = [
    "PLACED",
    "PACKED",
    "SHIPPED",
    "OUT_FOR_DELIVERY",
    "DELIVERED",
    "CANCELLED",
]

# Allowed next statuses.  Cancelling is only possible before the parcel
# leaves the warehouse, so the reserved stock can safely be put back.
DELIVERY_TRANSITIONS = {
    "PLACED": {"PACKED", "CANCELLED"},
    "PACKED": {"SHIPPED", "CANCELLED"},
    "SHIPPED": {"OUT_FOR_DELIVERY"},
    "OUT_FOR_DELIVERY": {"DELIVERED"},
    "DELIVERED": set(),
    "CANCELLED": set(),
}

RESTOCK_STATUSES = [
    "REQUESTED",
    "APPROVED",
    "SHIPPED",
    "RECEIVED",
    "REJECTED",
    "CANCELLED",
]

# Stock is added to the warehouse only on the transition to RECEIVED.
RESTOCK_TRANSITIONS = {
    "REQUESTED": {"APPROVED", "REJECTED", "CANCELLED"},
    "APPROVED": {"SHIPPED", "CANCELLED"},
    "SHIPPED": {"RECEIVED"},
    "RECEIVED": set(),
    "REJECTED": set(),
    "CANCELLED": set(),
}


# ============================================================
# DATABASE CONNECTION
# ============================================================

def get_connection():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

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
            idempotency_key TEXT,
            FOREIGN KEY (user_id)
                REFERENCES users(id),
            FOREIGN KEY (item_id)
                REFERENCES products(item_id),
            UNIQUE(user_id, idempotency_key)
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_user_id
            ON sessions(user_id);

        CREATE INDEX IF NOT EXISTS idx_orders_user_id
            ON orders(user_id);

        CREATE INDEX IF NOT EXISTS idx_orders_item_id
            ON orders(item_id);

        CREATE INDEX IF NOT EXISTS idx_sales_history_item_id
            ON sales_history(item_id);

        -- ----------------------------------------------------
        -- Multi-warehouse inventory
        -- ----------------------------------------------------

        CREATE TABLE IF NOT EXISTS warehouses (
            warehouse_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            location TEXT NOT NULL DEFAULT ''
        );

        -- products.stock is the total across all warehouses; this table
        -- holds the per-warehouse breakdown and always sums to it.
        CREATE TABLE IF NOT EXISTS warehouse_stock (
            warehouse_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            stock INTEGER NOT NULL CHECK (stock >= 0),
            PRIMARY KEY (warehouse_id, item_id),
            FOREIGN KEY (warehouse_id)
                REFERENCES warehouses(warehouse_id)
                ON DELETE CASCADE,
            FOREIGN KEY (item_id)
                REFERENCES products(item_id)
                ON DELETE CASCADE
        );

        -- Which warehouse(s) fulfilled each order.
        CREATE TABLE IF NOT EXISTS order_allocations (
            order_id INTEGER NOT NULL,
            warehouse_id TEXT NOT NULL,
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            PRIMARY KEY (order_id, warehouse_id),
            FOREIGN KEY (order_id)
                REFERENCES orders(id)
                ON DELETE CASCADE,
            FOREIGN KEY (warehouse_id)
                REFERENCES warehouses(warehouse_id)
        );

        -- Timeline of delivery updates for each order.
        CREATE TABLE IF NOT EXISTS delivery_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            note TEXT NOT NULL DEFAULT '',
            updated_by INTEGER,
            created_at TEXT NOT NULL,
            FOREIGN KEY (order_id)
                REFERENCES orders(id)
                ON DELETE CASCADE,
            FOREIGN KEY (updated_by)
                REFERENCES users(id)
        );

        -- ----------------------------------------------------
        -- Suppliers and restock requests
        -- ----------------------------------------------------

        CREATE TABLE IF NOT EXISTS suppliers (
            supplier_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            contact_email TEXT NOT NULL DEFAULT '',
            lead_time_days INTEGER NOT NULL DEFAULT 3
                CHECK (lead_time_days >= 0)
        );

        CREATE TABLE IF NOT EXISTS restock_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id TEXT NOT NULL,
            warehouse_id TEXT NOT NULL,
            supplier_id TEXT NOT NULL,
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            status TEXT NOT NULL,
            note TEXT NOT NULL DEFAULT '',
            requested_by INTEGER NOT NULL,
            idempotency_key TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            received_at TEXT,
            FOREIGN KEY (item_id)
                REFERENCES products(item_id),
            FOREIGN KEY (warehouse_id)
                REFERENCES warehouses(warehouse_id),
            FOREIGN KEY (supplier_id)
                REFERENCES suppliers(supplier_id),
            FOREIGN KEY (requested_by)
                REFERENCES users(id),
            UNIQUE (requested_by, idempotency_key)
        );

        CREATE TABLE IF NOT EXISTS restock_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            restock_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            note TEXT NOT NULL DEFAULT '',
            updated_by INTEGER,
            created_at TEXT NOT NULL,
            FOREIGN KEY (restock_id)
                REFERENCES restock_requests(id)
                ON DELETE CASCADE,
            FOREIGN KEY (updated_by)
                REFERENCES users(id)
        );

        CREATE INDEX IF NOT EXISTS idx_warehouse_stock_item_id
            ON warehouse_stock(item_id);

        CREATE INDEX IF NOT EXISTS idx_delivery_events_order_id
            ON delivery_events(order_id);

        CREATE INDEX IF NOT EXISTS idx_restock_requests_status
            ON restock_requests(status);

        CREATE INDEX IF NOT EXISTS idx_restock_events_restock_id
            ON restock_events(restock_id);
        """
    )

    _ensure_sales_history_tracking_column(conn)
    _record_legacy_orders_in_sales_history(conn)
    _ensure_idempotency_key_column(conn)
    _ensure_delivery_status_column(conn)
    _ensure_reference_data(conn)
    _ensure_warehouse_stock(conn)

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


def place_order(
    user_id: int,
    item_id: str,
    quantity: int,
    idempotency_key: str,
    warehouse_id: str | None = None,
):
    """
    Atomically place an order.

    If ``warehouse_id`` is given the whole order is fulfilled from that
    warehouse.  Otherwise it is fulfilled from the warehouses holding the
    most stock first, splitting across warehouses only when necessary.

    Returns a dictionary describing the result.
    """

    # Validate before touching the database.
    if quantity <= 0:
        return {
            "success": False,
            "status": "INVALID_QUANTITY",
            "message": "Quantity must be greater than zero.",
        }

    warehouse_id = (warehouse_id or "").strip() or None

    conn = get_connection()

    try:
        # BEGIN IMMEDIATE obtains a write transaction.
        #
        # This is important for concurrency:
        # only one writer can perform the stock-changing
        # transaction at a time.
        conn.execute("BEGIN IMMEDIATE")

        existing_order = conn.execute(
                    """
                    SELECT id, status
                    FROM orders
                    WHERE user_id = ?
                    AND idempotency_key = ?
                    """,
                    (user_id, idempotency_key),
                ).fetchone()
        
        if existing_order is not None:
            conn.rollback()
        
            return {
                        "success": True,
                        "status": existing_order["status"],
                        "message": f"Order #{existing_order['id']} was already processed.",
                        "order_id": existing_order["id"],
                }

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

        # Decide which warehouse(s) the units come from.
        allocation = _allocate_order_stock(
            conn,
            item_id,
            quantity,
            warehouse_id,
        )

        if not allocation["success"]:
            conn.rollback()

            return {
                "success": False,
                "status": allocation["status"],
                "message": allocation["message"].format(
                    product=product["name"]
                ),
            }

        for allocated_warehouse_id, allocated_quantity, _ in allocation["parts"]:
            conn.execute(
                """
                UPDATE warehouse_stock
                SET stock = stock - ?
                WHERE warehouse_id = ?
                  AND item_id = ?
                """,
                (allocated_quantity, allocated_warehouse_id, item_id),
            )

        # Decrease the total stock.
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

        created_at = utc_now()

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
                    sales_history_recorded,
                    idempotency_key,
                    delivery_status
                )
            VALUES
                (?, ?, ?, ?, ?, 1, ?, 'PLACED')
            """,
            (
                user_id,
                item_id,
                quantity,
                "SUCCESS",
                created_at,
                idempotency_key,
            ),
        )

        order_id = cursor.lastrowid

        conn.executemany(
            """
            INSERT INTO order_allocations
                (order_id, warehouse_id, quantity)
            VALUES
                (?, ?, ?)
            """,
            [
                (order_id, allocated_warehouse_id, allocated_quantity)
                for allocated_warehouse_id, allocated_quantity, _
                in allocation["parts"]
            ],
        )

        conn.execute(
            """
            INSERT INTO delivery_events
                (order_id, status, note, updated_by, created_at)
            VALUES
                (?, 'PLACED', 'Order placed', ?, ?)
            """,
            (order_id, user_id, created_at),
        )

        # Everything succeeded.
        conn.commit()

        fulfilled_from = ", ".join(
            f"{name} ({allocated_quantity})"
            for _, allocated_quantity, name in allocation["parts"]
        )

        message = (
            f"Order #{order_id} placed successfully. "
            f"Ordered {quantity} × {product['name']}. "
            f"Remaining stock: {remaining_stock}."
        )

        if fulfilled_from:
            message += f" Fulfilled from: {fulfilled_from}."

        return {
            "success": True,
            "status": "SUCCESS",
            "message": message,
            "order_id": order_id,
            "remaining_stock": remaining_stock,
            "allocations": [
                {
                    "warehouse_id": allocated_warehouse_id,
                    "warehouse_name": name,
                    "quantity": allocated_quantity,
                }
                for allocated_warehouse_id, allocated_quantity, name
                in allocation["parts"]
            ],
        }

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


def _allocate_order_stock(conn, item_id, quantity, warehouse_id=None):
    """
    Work out which warehouses fulfil an order.

    Must be called inside the order's write transaction.

    Returns {"success": True, "parts": [(warehouse_id, quantity, name), ...]}
    or {"success": False, "status": ..., "message": ...}.  Messages may
    contain "{product}", which the caller fills in.
    """
    rows = conn.execute(
        """
        SELECT
            warehouse_stock.warehouse_id,
            warehouses.name,
            warehouse_stock.stock
        FROM warehouse_stock
        JOIN warehouses
            ON warehouses.warehouse_id = warehouse_stock.warehouse_id
        WHERE warehouse_stock.item_id = ?
        ORDER BY warehouse_stock.stock DESC, warehouse_stock.warehouse_id
        """,
        (item_id,),
    ).fetchall()

    if warehouse_id is not None:
        warehouse = conn.execute(
            "SELECT name FROM warehouses WHERE warehouse_id = ?",
            (warehouse_id,),
        ).fetchone()

        if warehouse is None:
            return {
                "success": False,
                "status": "WAREHOUSE_NOT_FOUND",
                "message": "The selected warehouse does not exist.",
            }

        available = next(
            (row["stock"] for row in rows if row["warehouse_id"] == warehouse_id),
            0,
        )

        if available < quantity:
            return {
                "success": False,
                "status": "FAILED_INSUFFICIENT_STOCK",
                "message": (
                    f"Only {available} units of {{product}} are available "
                    f"in {warehouse['name']}."
                ),
            }

        return {
            "success": True,
            "parts": [(warehouse_id, quantity, warehouse["name"])],
        }

    # A product without any warehouse rows predates multi-warehouse
    # tracking; only the total stock is adjusted for it.
    if not rows:
        return {"success": True, "parts": []}

    parts = []
    remaining = quantity

    for row in rows:
        if remaining == 0:
            break

        take = min(row["stock"], remaining)

        if take > 0:
            parts.append((row["warehouse_id"], take, row["name"]))
            remaining -= take

    if remaining > 0:
        available = quantity - remaining

        return {
            "success": False,
            "status": "FAILED_INSUFFICIENT_STOCK",
            "message": (
                f"Only {available} units of {{product}} are available "
                f"across all warehouses."
            ),
        }

    return {"success": True, "parts": parts}


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

def _ensure_idempotency_key_column(conn):
    columns = conn.execute(
        "PRAGMA table_info(orders)"
    ).fetchall()

    if not any(
        column["name"] == "idempotency_key"
        for column in columns
    ):
        conn.execute(
            """
            ALTER TABLE orders
            ADD COLUMN idempotency_key TEXT
            """
        )

    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS
            idx_orders_user_id_idempotency_key
        ON orders(user_id, idempotency_key)
        WHERE idempotency_key IS NOT NULL
        AND idempotency_key <> ''
        """
    )


def _ensure_delivery_status_column(conn):
    """Add the delivery status to orders created before delivery tracking."""
    columns = conn.execute(
        "PRAGMA table_info(orders)"
    ).fetchall()

    if any(column["name"] == "delivery_status" for column in columns):
        return

    conn.execute(
        """
        ALTER TABLE orders
        ADD COLUMN delivery_status TEXT NOT NULL DEFAULT 'PLACED'
        """
    )


def _ensure_reference_data(conn):
    """Create the demo warehouses and suppliers if none exist yet."""
    from mock_data import SUPPLIERS, WAREHOUSES

    has_warehouses = conn.execute(
        "SELECT 1 FROM warehouses LIMIT 1"
    ).fetchone()

    if has_warehouses is None:
        conn.executemany(
            """
            INSERT INTO warehouses
                (warehouse_id, name, location)
            VALUES
                (?, ?, ?)
            """,
            [
                (warehouse.warehouse_id, warehouse.name, warehouse.location)
                for warehouse in WAREHOUSES
            ],
        )

    has_suppliers = conn.execute(
        "SELECT 1 FROM suppliers LIMIT 1"
    ).fetchone()

    if has_suppliers is None:
        conn.executemany(
            """
            INSERT INTO suppliers
                (supplier_id, name, contact_email, lead_time_days)
            VALUES
                (?, ?, ?, ?)
            """,
            [
                (
                    supplier.supplier_id,
                    supplier.name,
                    supplier.contact_email,
                    supplier.lead_time_days,
                )
                for supplier in SUPPLIERS
            ],
        )


def _ensure_warehouse_stock(conn):
    """
    Give every product a stock row in every warehouse.

    A product with no warehouse rows at all (for example from a database
    created before multi-warehouse tracking) has its existing total split
    across the warehouses, so the per-warehouse rows add up to
    products.stock.  Missing product/warehouse pairs are added with zero
    stock.
    """
    from mock_data import (
        WAREHOUSES,
        WarehouseSeed,
        split_stock_across_warehouses,
    )

    warehouse_rows = conn.execute(
        """
        SELECT warehouse_id, name, location
        FROM warehouses
        ORDER BY rowid
        """
    ).fetchall()

    if not warehouse_rows:
        return

    seed_shares = {
        warehouse.warehouse_id: warehouse.stock_share_percent
        for warehouse in WAREHOUSES
    }

    # Unknown warehouses get an even share.
    even_share = 100 // len(warehouse_rows)

    warehouses = [
        WarehouseSeed(
            row["warehouse_id"],
            row["name"],
            row["location"],
            seed_shares.get(row["warehouse_id"], even_share),
        )
        for row in warehouse_rows
    ]

    products = conn.execute(
        """
        SELECT item_id, stock
        FROM products
        WHERE item_id NOT IN (
            SELECT DISTINCT item_id FROM warehouse_stock
        )
        """
    ).fetchall()

    for product in products:
        conn.executemany(
            """
            INSERT INTO warehouse_stock
                (warehouse_id, item_id, stock)
            VALUES
                (?, ?, ?)
            """,
            [
                (warehouse_id, product["item_id"], stock)
                for warehouse_id, stock in split_stock_across_warehouses(
                    int(product["stock"]),
                    warehouses,
                )
            ],
        )

    conn.execute(
        """
        INSERT OR IGNORE INTO warehouse_stock
            (warehouse_id, item_id, stock)
        SELECT warehouses.warehouse_id, products.item_id, 0
        FROM warehouses
        CROSS JOIN products
        """
    )


# ============================================================
# WAREHOUSES
# ============================================================

def get_warehouse_stock():
    """
    Return warehouses and a per-product stock breakdown.

    Example:

        {
            "warehouses": [
                {"warehouse_id": "wh_blr", "name": ..., "location": ...,
                 "total_units": 120, "low_stock_items": 2}
            ],
            "products": [
                {"item_id": "item_101", "name": "Laptop", "total_stock": 50,
                 "by_warehouse": {"wh_blr": 25, "wh_mum": 15, "wh_del": 10}}
            ],
            "low_stock_threshold": 5
        }
    """
    conn = get_connection()

    try:
        warehouse_rows = conn.execute(
            """
            SELECT
                warehouses.warehouse_id,
                warehouses.name,
                warehouses.location,
                COALESCE(SUM(warehouse_stock.stock), 0) AS total_units,
                COALESCE(
                    SUM(
                        CASE
                            WHEN warehouse_stock.stock <= ? THEN 1
                            ELSE 0
                        END
                    ),
                    0
                ) AS low_stock_items
            FROM warehouses
            LEFT JOIN warehouse_stock
                ON warehouse_stock.warehouse_id = warehouses.warehouse_id
            GROUP BY warehouses.warehouse_id
            ORDER BY warehouses.rowid
            """,
            (LOW_STOCK_THRESHOLD,),
        ).fetchall()

        product_rows = conn.execute(
            """
            SELECT item_id, name, stock
            FROM products
            ORDER BY item_id
            """
        ).fetchall()

        stock_rows = conn.execute(
            """
            SELECT warehouse_id, item_id, stock
            FROM warehouse_stock
            """
        ).fetchall()

    finally:
        conn.close()

    stock_by_item = {}

    for row in stock_rows:
        stock_by_item.setdefault(row["item_id"], {})[row["warehouse_id"]] = (
            int(row["stock"])
        )

    return {
        "warehouses": [
            {
                "warehouse_id": row["warehouse_id"],
                "name": row["name"],
                "location": row["location"],
                "total_units": int(row["total_units"]),
                "low_stock_items": int(row["low_stock_items"]),
            }
            for row in warehouse_rows
        ],
        "products": [
            {
                "item_id": row["item_id"],
                "name": row["name"],
                "total_stock": int(row["stock"]),
                "by_warehouse": stock_by_item.get(row["item_id"], {}),
            }
            for row in product_rows
        ],
        "low_stock_threshold": LOW_STOCK_THRESHOLD,
    }


# ============================================================
# SUPPLIERS AND RESTOCK REQUESTS
# ============================================================

def get_suppliers():
    conn = get_connection()

    try:
        rows = conn.execute(
            """
            SELECT supplier_id, name, contact_email, lead_time_days
            FROM suppliers
            ORDER BY name
            """
        ).fetchall()

        return [dict(row) for row in rows]

    finally:
        conn.close()


def create_restock_request(
    user_id: int,
    item_id: str,
    warehouse_id: str,
    supplier_id: str,
    quantity: int,
    idempotency_key: str,
    note: str = "",
):
    """Ask a supplier to deliver more of a product to one warehouse."""

    if quantity <= 0:
        return {
            "success": False,
            "status": "INVALID_QUANTITY",
            "message": "Restock quantity must be greater than zero.",
        }

    note = (note or "").strip()[:500]

    conn = get_connection()

    try:
        conn.execute("BEGIN IMMEDIATE")

        existing = conn.execute(
            """
            SELECT id, status
            FROM restock_requests
            WHERE requested_by = ?
              AND idempotency_key = ?
            """,
            (user_id, idempotency_key),
        ).fetchone()

        if existing is not None:
            conn.rollback()

            return {
                "success": True,
                "status": "SUCCESS",
                "message": (
                    f"Restock request #{existing['id']} was already "
                    f"submitted ({existing['status']})."
                ),
                "restock_id": existing["id"],
            }

        product = conn.execute(
            "SELECT name FROM products WHERE item_id = ?",
            (item_id,),
        ).fetchone()

        if product is None:
            conn.rollback()
            return {
                "success": False,
                "status": "ITEM_NOT_FOUND",
                "message": "The requested product does not exist.",
            }

        warehouse = conn.execute(
            "SELECT name FROM warehouses WHERE warehouse_id = ?",
            (warehouse_id,),
        ).fetchone()

        if warehouse is None:
            conn.rollback()
            return {
                "success": False,
                "status": "WAREHOUSE_NOT_FOUND",
                "message": "The selected warehouse does not exist.",
            }

        supplier = conn.execute(
            "SELECT name FROM suppliers WHERE supplier_id = ?",
            (supplier_id,),
        ).fetchone()

        if supplier is None:
            conn.rollback()
            return {
                "success": False,
                "status": "SUPPLIER_NOT_FOUND",
                "message": "The selected supplier does not exist.",
            }

        now = utc_now()

        cursor = conn.execute(
            """
            INSERT INTO restock_requests
                (
                    item_id,
                    warehouse_id,
                    supplier_id,
                    quantity,
                    status,
                    note,
                    requested_by,
                    idempotency_key,
                    created_at,
                    updated_at
                )
            VALUES
                (?, ?, ?, ?, 'REQUESTED', ?, ?, ?, ?, ?)
            """,
            (
                item_id,
                warehouse_id,
                supplier_id,
                quantity,
                note,
                user_id,
                idempotency_key,
                now,
                now,
            ),
        )

        restock_id = cursor.lastrowid

        conn.execute(
            """
            INSERT INTO restock_events
                (restock_id, status, note, updated_by, created_at)
            VALUES
                (?, 'REQUESTED', ?, ?, ?)
            """,
            (restock_id, note or "Restock requested", user_id, now),
        )

        conn.commit()

        return {
            "success": True,
            "status": "SUCCESS",
            "message": (
                f"Restock request #{restock_id} sent to {supplier['name']}: "
                f"{quantity} × {product['name']} for {warehouse['name']}."
            ),
            "restock_id": restock_id,
        }

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


def update_restock_status(
    restock_id: int,
    new_status: str,
    user_id: int,
    note: str = "",
):
    """
    Move a restock request to its next status.

    Receiving a shipment adds the stock to the warehouse and to the product
    total in the same transaction.  Repeating the current status is
    accepted and changes nothing, so retried requests are safe.
    """
    new_status = (new_status or "").strip().upper()
    note = (note or "").strip()[:500]

    if new_status not in RESTOCK_STATUSES:
        return {
            "success": False,
            "status": "INVALID_STATUS",
            "message": f"Unknown restock status: {new_status or '(empty)'}.",
        }

    conn = get_connection()

    try:
        conn.execute("BEGIN IMMEDIATE")

        request = conn.execute(
            """
            SELECT
                restock_requests.id,
                restock_requests.item_id,
                restock_requests.warehouse_id,
                restock_requests.quantity,
                restock_requests.status,
                products.name AS product_name,
                warehouses.name AS warehouse_name
            FROM restock_requests
            JOIN products
                ON products.item_id = restock_requests.item_id
            JOIN warehouses
                ON warehouses.warehouse_id = restock_requests.warehouse_id
            WHERE restock_requests.id = ?
            """,
            (restock_id,),
        ).fetchone()

        if request is None:
            conn.rollback()
            return {
                "success": False,
                "status": "RESTOCK_NOT_FOUND",
                "message": f"Restock request #{restock_id} does not exist.",
            }

        current_status = request["status"]

        if current_status == new_status:
            conn.rollback()
            return {
                "success": True,
                "status": "SUCCESS",
                "message": (
                    f"Restock request #{restock_id} is already {new_status}."
                ),
            }

        if new_status not in RESTOCK_TRANSITIONS[current_status]:
            conn.rollback()
            return {
                "success": False,
                "status": "INVALID_TRANSITION",
                "message": (
                    f"Restock request #{restock_id} cannot move from "
                    f"{current_status} to {new_status}."
                ),
            }

        now = utc_now()

        if new_status == "RECEIVED":
            conn.execute(
                """
                INSERT INTO warehouse_stock
                    (warehouse_id, item_id, stock)
                VALUES
                    (?, ?, ?)
                ON CONFLICT (warehouse_id, item_id)
                DO UPDATE SET stock = stock + excluded.stock
                """,
                (
                    request["warehouse_id"],
                    request["item_id"],
                    request["quantity"],
                ),
            )

            conn.execute(
                """
                UPDATE products
                SET stock = stock + ?
                WHERE item_id = ?
                """,
                (request["quantity"], request["item_id"]),
            )

        conn.execute(
            """
            UPDATE restock_requests
            SET status = ?,
                updated_at = ?,
                received_at = CASE WHEN ? = 'RECEIVED' THEN ? ELSE received_at END
            WHERE id = ?
            """,
            (new_status, now, new_status, now, restock_id),
        )

        conn.execute(
            """
            INSERT INTO restock_events
                (restock_id, status, note, updated_by, created_at)
            VALUES
                (?, ?, ?, ?, ?)
            """,
            (restock_id, new_status, note, user_id, now),
        )

        conn.commit()

        message = f"Restock request #{restock_id} is now {new_status}."

        if new_status == "RECEIVED":
            message += (
                f" Added {request['quantity']} × {request['product_name']} "
                f"to {request['warehouse_name']}."
            )

        return {
            "success": True,
            "status": "SUCCESS",
            "message": message,
            "stock_changed": new_status == "RECEIVED",
        }

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


def get_restock_requests(limit: int = 50):
    """Return the most recent restock requests, newest first."""
    conn = get_connection()

    try:
        rows = conn.execute(
            """
            SELECT
                restock_requests.id,
                restock_requests.item_id,
                products.name AS product_name,
                restock_requests.warehouse_id,
                warehouses.name AS warehouse_name,
                restock_requests.supplier_id,
                suppliers.name AS supplier_name,
                suppliers.lead_time_days,
                restock_requests.quantity,
                restock_requests.status,
                restock_requests.note,
                users.username AS requested_by,
                restock_requests.created_at,
                restock_requests.updated_at,
                restock_requests.received_at
            FROM restock_requests
            JOIN products
                ON products.item_id = restock_requests.item_id
            JOIN warehouses
                ON warehouses.warehouse_id = restock_requests.warehouse_id
            JOIN suppliers
                ON suppliers.supplier_id = restock_requests.supplier_id
            LEFT JOIN users
                ON users.id = restock_requests.requested_by
            ORDER BY restock_requests.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    finally:
        conn.close()

    requests = []

    for row in rows:
        request = dict(row)
        request["expected_by"] = _add_days(
            row["created_at"],
            row["lead_time_days"],
        )
        request["next_statuses"] = sorted(
            RESTOCK_TRANSITIONS.get(row["status"], set()),
            key=RESTOCK_STATUSES.index,
        )
        requests.append(request)

    return requests


def _add_days(iso_timestamp, days):
    try:
        moment = datetime.fromisoformat(iso_timestamp)
    except (TypeError, ValueError):
        return ""

    return (moment + timedelta(days=int(days))).date().isoformat()


# ============================================================
# DELIVERY TRACKING
# ============================================================

def update_delivery_status(
    order_id: int,
    new_status: str,
    user_id: int,
    note: str = "",
):
    """
    Move an order to its next delivery status.

    Cancelling an order that has not shipped yet returns its units to the
    warehouses they were taken from.  Repeating the current status is
    accepted and changes nothing.
    """
    new_status = (new_status or "").strip().upper()
    note = (note or "").strip()[:500]

    if new_status not in DELIVERY_STATUSES:
        return {
            "success": False,
            "status": "INVALID_STATUS",
            "message": f"Unknown delivery status: {new_status or '(empty)'}.",
        }

    conn = get_connection()

    try:
        conn.execute("BEGIN IMMEDIATE")

        order = conn.execute(
            """
            SELECT id, item_id, quantity, status, delivery_status
            FROM orders
            WHERE id = ?
            """,
            (order_id,),
        ).fetchone()

        if order is None:
            conn.rollback()
            return {
                "success": False,
                "status": "ORDER_NOT_FOUND",
                "message": f"Order #{order_id} does not exist.",
            }

        current_status = order["delivery_status"]

        if current_status == new_status:
            conn.rollback()
            return {
                "success": True,
                "status": "SUCCESS",
                "message": f"Order #{order_id} is already {new_status}.",
            }

        if new_status not in DELIVERY_TRANSITIONS.get(current_status, set()):
            conn.rollback()
            return {
                "success": False,
                "status": "INVALID_TRANSITION",
                "message": (
                    f"Order #{order_id} cannot move from "
                    f"{current_status} to {new_status}."
                ),
            }

        now = utc_now()

        if new_status == "CANCELLED" and order["status"] == "SUCCESS":
            _return_order_stock(conn, order)

        conn.execute(
            """
            UPDATE orders
            SET delivery_status = ?
            WHERE id = ?
            """,
            (new_status, order_id),
        )

        conn.execute(
            """
            INSERT INTO delivery_events
                (order_id, status, note, updated_by, created_at)
            VALUES
                (?, ?, ?, ?, ?)
            """,
            (order_id, new_status, note, user_id, now),
        )

        conn.commit()

        message = f"Order #{order_id} is now {new_status.replace('_', ' ')}."

        if new_status == "CANCELLED":
            message += f" {order['quantity']} units returned to stock."

        return {
            "success": True,
            "status": "SUCCESS",
            "message": message,
            "stock_changed": new_status == "CANCELLED",
        }

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


def _return_order_stock(conn, order):
    """Put a cancelled order's units back where they came from."""
    allocations = conn.execute(
        """
        SELECT warehouse_id, quantity
        FROM order_allocations
        WHERE order_id = ?
        """,
        (order["id"],),
    ).fetchall()

    for allocation in allocations:
        conn.execute(
            """
            INSERT INTO warehouse_stock
                (warehouse_id, item_id, stock)
            VALUES
                (?, ?, ?)
            ON CONFLICT (warehouse_id, item_id)
            DO UPDATE SET stock = stock + excluded.stock
            """,
            (
                allocation["warehouse_id"],
                order["item_id"],
                allocation["quantity"],
            ),
        )

    conn.execute(
        """
        UPDATE products
        SET stock = stock + ?
        WHERE item_id = ?
        """,
        (order["quantity"], order["item_id"]),
    )

    # The cancelled order is no longer a sale.  Take it back out of the most
    # recent daily observation so forecasts stay consistent with stock.
    latest_history = conn.execute(
        """
        SELECT id
        FROM sales_history
        WHERE item_id = ?
        ORDER BY day_index DESC, id DESC
        LIMIT 1
        """,
        (order["item_id"],),
    ).fetchone()

    if latest_history is not None:
        conn.execute(
            """
            UPDATE sales_history
            SET units_sold = MAX(units_sold - ?, 0)
            WHERE id = ?
            """,
            (order["quantity"], latest_history["id"]),
        )


def get_deliveries(limit: int = 30):
    """
    Return recent orders from all users with their delivery status,
    fulfilling warehouses and update timeline, newest first.
    """
    conn = get_connection()

    try:
        order_rows = conn.execute(
            """
            SELECT
                orders.id,
                orders.item_id,
                products.name AS product_name,
                orders.quantity,
                orders.delivery_status,
                orders.created_at,
                users.username
            FROM orders
            JOIN products
                ON products.item_id = orders.item_id
            LEFT JOIN users
                ON users.id = orders.user_id
            WHERE orders.status = 'SUCCESS'
            ORDER BY orders.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        order_ids = [row["id"] for row in order_rows]
        allocation_rows = []
        event_rows = []

        if order_ids:
            placeholders = ", ".join("?" for _ in order_ids)

            allocation_rows = conn.execute(
                f"""
                SELECT
                    order_allocations.order_id,
                    order_allocations.warehouse_id,
                    warehouses.name AS warehouse_name,
                    order_allocations.quantity
                FROM order_allocations
                JOIN warehouses
                    ON warehouses.warehouse_id = order_allocations.warehouse_id
                WHERE order_allocations.order_id IN ({placeholders})
                ORDER BY order_allocations.quantity DESC
                """,
                order_ids,
            ).fetchall()

            event_rows = conn.execute(
                f"""
                SELECT
                    delivery_events.order_id,
                    delivery_events.status,
                    delivery_events.note,
                    delivery_events.created_at,
                    users.username AS updated_by
                FROM delivery_events
                LEFT JOIN users
                    ON users.id = delivery_events.updated_by
                WHERE delivery_events.order_id IN ({placeholders})
                ORDER BY delivery_events.id
                """,
                order_ids,
            ).fetchall()

    finally:
        conn.close()

    allocations_by_order = {}

    for row in allocation_rows:
        allocations_by_order.setdefault(row["order_id"], []).append(
            {
                "warehouse_id": row["warehouse_id"],
                "warehouse_name": row["warehouse_name"],
                "quantity": int(row["quantity"]),
            }
        )

    events_by_order = {}

    for row in event_rows:
        events_by_order.setdefault(row["order_id"], []).append(
            {
                "status": row["status"],
                "note": row["note"],
                "created_at": row["created_at"],
                "updated_by": row["updated_by"],
            }
        )

    deliveries = []

    for row in order_rows:
        events = events_by_order.get(row["id"], [])

        deliveries.append(
            {
                "order_id": row["id"],
                "item_id": row["item_id"],
                "product_name": row["product_name"],
                "quantity": int(row["quantity"]),
                "username": row["username"],
                "delivery_status": row["delivery_status"],
                "created_at": row["created_at"],
                "last_updated": (
                    events[-1]["created_at"] if events else row["created_at"]
                ),
                "allocations": allocations_by_order.get(row["id"], []),
                "events": events,
                "next_statuses": sorted(
                    DELIVERY_TRANSITIONS.get(row["delivery_status"], set()),
                    key=DELIVERY_STATUSES.index,
                ),
            }
        )

    return deliveries


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    initialize_database()
    print(f"Database initialized at: {DB_PATH}")
