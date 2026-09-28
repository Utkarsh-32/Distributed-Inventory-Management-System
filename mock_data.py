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