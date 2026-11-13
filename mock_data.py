from dataclasses import dataclass


@dataclass(frozen=True)
class ProductSeed:
    item_id: str
    name: str
    initial_stock: int


PRODUCTS = [
    ProductSeed("item_101", "Laptop", 50),
    ProductSeed("item_102", "Wireless Mouse", 20),
    ProductSeed("item_103", "Keyboard", 35),
    ProductSeed("item_104", "Monitor", 30),
    ProductSeed("item_105", "Headphones", 25),
    ProductSeed("item_106", "Webcam", 25),
    ProductSeed("item_107", "USB-C Charger", 35),
    ProductSeed("item_108", "SSD", 15),
    ProductSeed("item_109", "Mechanical Keyboard", 30),
    ProductSeed("item_110", "Laptop Stand", 25),
    ProductSeed("item_111", "HDMI Cable", 55),
    ProductSeed("item_112", "Power Bank", 20),
]


@dataclass(frozen=True)
class WarehouseSeed:
    warehouse_id: str
    name: str
    location: str
    # Share of each product's initial stock held here, in percent.
    stock_share_percent: int


WAREHOUSES = [
    WarehouseSeed("wh_blr", "Bengaluru Central", "Bengaluru, KA", 50),
    WarehouseSeed("wh_mum", "Mumbai West", "Mumbai, MH", 30),
    WarehouseSeed("wh_del", "Delhi North", "Delhi, DL", 20),
]


@dataclass(frozen=True)
class SupplierSeed:
    supplier_id: str
    name: str
    contact_email: str
    lead_time_days: int


SUPPLIERS = [
    SupplierSeed("sup_techsource", "TechSource Distributors", "orders@techsource.example", 3),
    SupplierSeed("sup_peripheral", "Peripheral Hub", "supply@peripheralhub.example", 5),
    SupplierSeed("sup_quickparts", "QuickParts Express", "dispatch@quickparts.example", 1),
]


def split_stock_across_warehouses(total_stock, warehouses=WAREHOUSES):
    """
    Split one product's stock across warehouses by their configured share.

    Rounding leftovers go to the first warehouse, so the parts always add up
    to exactly ``total_stock``.

    Returns:
        List of (warehouse_id, stock) tuples.
    """
    if not warehouses:
        return []

    parts = [
        (warehouse.warehouse_id, total_stock * warehouse.stock_share_percent // 100)
        for warehouse in warehouses
    ]

    leftover = total_stock - sum(stock for _, stock in parts)
    first_id, first_stock = parts[0]
    parts[0] = (first_id, first_stock + leftover)

    return parts


def make_series(base, trend=0, noise=(0,)):
    """
    Generate 30 deterministic daily sales values.

    trend:
        Positive -> gradually increasing demand
        Negative -> gradually decreasing demand
        Zero     -> roughly stable demand

    noise:
        Small repeating variations to avoid perfectly flat data.
    """
    values = []

    for day in range(30):
        trend_component = (trend * day) // 5
        noise_component = noise[day % len(noise)]

        sales = max(
            0,
            base + trend_component + noise_component
        )

        values.append(sales)

    return values


SALES_HISTORY = {
    # Stable
    "item_101": make_series(
        4,
        noise=(0, 1, -1, 0, 0, 1, -1)
    ),

    # Increasing
    "item_102": make_series(
        4,
        trend=1,
        noise=(0, 1, 0, -1, 1, 0, 2)
    ),

    # Stable
    "item_103": make_series(
        5,
        noise=(0, 0, 1, -1, 0, 1, -1)
    ),

    # Decreasing
    "item_104": make_series(
        4,
        trend=-1,
        noise=(0, 0, -1, 1, 0)
    ),

    # Volatile + decreasing
    "item_105": make_series(
        6,
        trend=-1,
        noise=(3, -2, 1, 0, -3, 2, 4)
    ),

    # Mostly stable with occasional spike
    "item_106": make_series(
        3,
        noise=(0, 0, 0, 7, 0, 0, 1)
    ),

    # Increasing
    "item_107": make_series(
        5,
        trend=1,
        noise=(0, 1, 0, 2, -1, 1, 0)
    ),

    # Low and stable
    "item_108": make_series(
        2,
        noise=(0, 0, 1, 0, -1)
    ),

    # Increasing + volatile
    "item_109": make_series(
        5,
        trend=1,
        noise=(2, -1, 3, -2, 1, 0, 2)
    ),

    # Decreasing
    "item_110": make_series(
        3,
        trend=-1,
        noise=(0, 0, 1, -1, 0)
    ),

    # Stable
    "item_111": make_series(
        7,
        noise=(0, 1, -1, 0, 1, 0, -1)
    ),

    # Increasing
    "item_112": make_series(
        5,
        trend=1,
        noise=(0, 0, 2, -1, 1, 0, 2)
    ),
}


def get_all_history_rows():
    """
    Convert the dictionary into rows suitable for SQLite insertion.

    Returns:
        List of tuples:
        (item_id, day_index, units_sold)
    """
    rows = []

    for item_id, daily_sales in SALES_HISTORY.items():
        for day_index, units_sold in enumerate(
            daily_sales,
            start=1
        ):
            rows.append(
                (item_id, day_index, units_sold)
            )

    return rows