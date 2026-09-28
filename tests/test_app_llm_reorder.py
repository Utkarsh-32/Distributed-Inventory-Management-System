import json
import grpc

import inventory_pb2
import inventory_pb2_grpc


APP_SERVER_ADDRESS = "localhost:50052"


def main():

    print("=" * 40)
    print("APPLICATION → LLM REORDER TEST")
    print("=" * 40)

    channel = grpc.insecure_channel(
        APP_SERVER_ADDRESS
    )

    stub = inventory_pb2_grpc.ClientServiceStub(
        channel
    )

    # --------------------------------------------------
    # Login
    # --------------------------------------------------

    login_response = stub.login(
        inventory_pb2.LoginRequest(
            username="alice",
            password="password",
        )
    )

    if login_response.status != "SUCCESS":
        raise RuntimeError(
            f"Login failed: {login_response.status}"
        )

    token = login_response.token

    print("Login successful.")

    # --------------------------------------------------
    # Reorder request
    # --------------------------------------------------

    response = stub.get(
        inventory_pb2.GetRequest(
            token=token,
            type="llm_reorder_suggestion",
        )
    )

    print()
    print("Application Server status:")
    print(response.status)

    if response.status != "SUCCESS":
        raise RuntimeError(response.status)

    raw = response.items[0].data

    print()
    print("Raw reorder response:")
    print(raw)

    payload = json.loads(raw)

    recommendations = payload.get(
        "recommendations"
    )

    if not isinstance(recommendations, list):
        raise RuntimeError(
            "Missing recommendations array."
        )

    print()

    for item in recommendations:

        print(
            f"{item['product_name']} "
            f"({item['item_id']})"
        )

        print(
            f"Current stock: "
            f"{item['current_stock']}"
        )

        print(
            f"Predicted next 7 days: "
            f"{item['predicted_7_day_demand']}"
        )

        print(
            f"Reorder: {item['reorder']}"
        )

        print(
            f"Reorder quantity: "
            f"{item['reorder_quantity']}"
        )

        print(
            f"Reason: {item['reason']}"
        )

        print()

    # --------------------------------------------------
    # Logout
    # --------------------------------------------------

    logout_response = stub.logout(
        inventory_pb2.LogoutRequest(
            token=token
        )
    )

    print(
        f"Logout: {logout_response.status}"
    )

    print()
    print("=" * 40)
    print("REORDER INTEGRATION TEST SUCCESSFUL")
    print("=" * 40)


if __name__ == "__main__":
    main()