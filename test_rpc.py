import json

import grpc

import inventory_pb2
import inventory_pb2_grpc


APP_SERVER_ADDRESS = "localhost:50052"


def main():

    channel = grpc.insecure_channel(
        APP_SERVER_ADDRESS
    )

    stub = inventory_pb2_grpc.ClientServiceStub(
        channel
    )

    # ========================================================
    # LOGIN
    # ========================================================

    print("=== LOGIN TEST ===")

    login_response = stub.login(
        inventory_pb2.LoginRequest(
            username="alice",
            password="password",
        )
    )

    print(
        "Status:",
        login_response.status,
    )

    if not login_response.token:
        print("Login failed.")
        return

    token = login_response.token

    print(
        "Token received:",
        token[:12] + "...",
    )

    # ========================================================
    # GET INVENTORY
    # ========================================================

    print()
    print("=== INITIAL INVENTORY ===")

    inventory_response = stub.get(
        inventory_pb2.GetRequest(
            token=token,
            type="inventory",
        )
    )

    for item in inventory_response.items:

        print(
            item.id,
            "->",
            item.data,
        )

    # ========================================================
    # PLACE ORDER
    # ========================================================

    print()
    print("=== PLACE ORDER ===")

    order_payload = json.dumps(
        {
            "item_id": "item_102",
            "quantity": 4,
        }
    )

    order_response = stub.post(
        inventory_pb2.PostRequest(
            token=token,
            type="order",
            data=order_payload,
        )
    )

    print(
        "Order status:",
        order_response.status,
    )

    print(
        "Message:",
        order_response.message,
    )

    # ========================================================
    # GET INVENTORY AGAIN
    # ========================================================

    print()
    print("=== INVENTORY AFTER ORDER ===")

    inventory_response = stub.get(
        inventory_pb2.GetRequest(
            token=token,
            type="inventory",
        )
    )

    for item in inventory_response.items:

        if item.id == "item_102":
            print(
                item.id,
                "->",
                item.data,
            )

    # ========================================================
    # TRY TOO MANY
    # ========================================================

    print()
    print("=== INSUFFICIENT STOCK TEST ===")

    bad_order_payload = json.dumps(
        {
            "item_id": "item_102",
            "quantity": 100,
        }
    )

    bad_order_response = stub.post(
        inventory_pb2.PostRequest(
            token=token,
            type="order",
            data=bad_order_payload,
        )
    )

    print(
        "Order status:",
        bad_order_response.status,
    )

    print(
        "Message:",
        bad_order_response.message,
    )

    # ========================================================
    # INVALID QUANTITY
    # ========================================================

    print()
    print("=== INVALID QUANTITY TEST ===")

    invalid_payload = json.dumps(
        {
            "item_id": "item_102",
            "quantity": -5,
        }
    )

    invalid_response = stub.post(
        inventory_pb2.PostRequest(
            token=token,
            type="order",
            data=invalid_payload,
        )
    )

    print(
        "Order status:",
        invalid_response.status,
    )

    print(
        "Message:",
        invalid_response.message,
    )

    # ========================================================
    # LOGOUT
    # ========================================================

    print()
    print("=== LOGOUT ===")

    logout_response = stub.logout(
        inventory_pb2.LogoutRequest(
            token=token,
        )
    )

    print(
        "Logout status:",
        logout_response.status,
    )

    unauthorized_response = stub.get(
    inventory_pb2.GetRequest(
        token=token,
        type="inventory",
    )
)

    print(
        "Request after logout:",
        unauthorized_response.status,
    )


if __name__ == "__main__":
    main()