from concurrent.futures import ThreadPoolExecutor

import grpc

import inventory_pb2
import inventory_pb2_grpc


APP_SERVER_ADDRESS = "localhost:50052"

ITEM_ID = "item_102"

EXPECTED_INITIAL_STOCK = 20

CONCURRENT_ORDERS = 24

ORDER_QUANTITY = 1


def get_stock(stub, token):
    """
    Ask the Application Server for the current
    Wireless Mouse stock.
    """

    response = stub.get(
        inventory_pb2.GetRequest(
            token=token,
            type="inventory",
        ),
        timeout=10,
    )

    if response.status != "SUCCESS":
        raise RuntimeError(
            f"Inventory request failed: {response.status}"
        )

    for item in response.items:

        if item.id == ITEM_ID:

            # Example:
            # Name: Wireless Mouse | Stock: 20

            return int(
                item.data.split(
                    " | Stock: ",
                    1,
                )[1]
            )

    raise RuntimeError(
        "Wireless Mouse was not found."
    )


def place_one_order(stub, token):
    """
    Submit one one-unit order.
    """

    response = stub.post(
        inventory_pb2.PostRequest(
            token=token,
            type="order",
            data=(
                "{"
                f'"item_id":"{ITEM_ID}",'
                f'"quantity":{ORDER_QUANTITY}'
                "}"
            ),
        ),
        timeout=20,
    )

    return response.status


def main():

    print("========================================")
    print("Concurrency Test")
    print("========================================")

    # --------------------------------------------------------
    # Connect to Application Server
    # --------------------------------------------------------

    channel = grpc.insecure_channel(
        APP_SERVER_ADDRESS
    )

    stub = inventory_pb2_grpc.ClientServiceStub(
        channel
    )

    # --------------------------------------------------------
    # Login
    # --------------------------------------------------------

    login_response = stub.login(
        inventory_pb2.LoginRequest(
            username="alice",
            password="password",
        ),
        timeout=10,
    )

    if login_response.status != "SUCCESS":

        raise RuntimeError(
            f"Login failed: "
            f"{login_response.status}"
        )

    token = login_response.token

    print("Login: SUCCESS")

    # --------------------------------------------------------
    # Check initial stock
    # --------------------------------------------------------

    starting_stock = get_stock(
        stub,
        token,
    )

    print(
        f"Starting stock: {starting_stock}"
    )

    if starting_stock != EXPECTED_INITIAL_STOCK:

        raise RuntimeError(
            f"Expected fresh stock "
            f"{EXPECTED_INITIAL_STOCK}, "
            f"but found {starting_stock}. "
            f"Run 'python seed_db.py' first."
        )

    # --------------------------------------------------------
    # Send concurrent orders
    # --------------------------------------------------------

    print(
        f"Sending {CONCURRENT_ORDERS} "
        f"simultaneous orders..."
    )

    with ThreadPoolExecutor(
        max_workers=CONCURRENT_ORDERS
    ) as executor:

        futures = [
            executor.submit(
                place_one_order,
                stub,
                token,
            )
            for _ in range(CONCURRENT_ORDERS)
        ]

        results = [
            future.result()
            for future in futures
        ]

    # --------------------------------------------------------
    # Count results
    # --------------------------------------------------------

    successes = sum(
        result == "SUCCESS"
        for result in results
    )

    insufficient_stock = sum(
        result == "FAILED_INSUFFICIENT_STOCK"
        for result in results
    )

    # --------------------------------------------------------
    # Check final stock
    # --------------------------------------------------------

    final_stock = get_stock(
        stub,
        token,
    )

    print()
    print("========================================")
    print("RESULT")
    print("========================================")

    print(
        f"Initial stock:            "
        f"{starting_stock}"
    )

    print(
        f"Concurrent orders:        "
        f"{CONCURRENT_ORDERS}"
    )

    print(
        f"Successful orders:        "
        f"{successes}"
    )

    print(
        f"Insufficient-stock rejects:"
        f" {insufficient_stock}"
    )

    print(
        f"Final stock:              "
        f"{final_stock}"
    )

    print("========================================")

    # --------------------------------------------------------
    # Verify expected behaviour
    # --------------------------------------------------------

    expected_successes = (
        EXPECTED_INITIAL_STOCK
        // ORDER_QUANTITY
    )

    expected_failures = (
        CONCURRENT_ORDERS
        - expected_successes
    )

    assert successes == expected_successes, (
        f"Expected {expected_successes} successes, "
        f"got {successes}"
    )

    assert insufficient_stock == expected_failures, (
        f"Expected {expected_failures} "
        f"rejections, "
        f"got {insufficient_stock}"
    )

    assert final_stock == 0, (
        f"Expected final stock 0, "
        f"got {final_stock}"
    )

    print(
        "PASS: concurrent orders did not "
        "oversell the inventory."
    )


if __name__ == "__main__":
    main()