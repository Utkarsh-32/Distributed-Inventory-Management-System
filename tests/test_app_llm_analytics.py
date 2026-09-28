import json
import grpc

import inventory_pb2
import inventory_pb2_grpc


APP_SERVER_ADDRESS = "localhost:50052"


def main():

    print("=" * 40)
    print("APPLICATION → LLM ANALYTICS TEST")
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
            f"Login failed: "
            f"{login_response.status}"
        )

    token = login_response.token

    print("Login successful.")

    # --------------------------------------------------
    # Analytics request
    # --------------------------------------------------

    response = stub.get(
        inventory_pb2.GetRequest(
            token=token,
            type="llm_analytics",
        )
    )

    print()
    print("Application Server status:")
    print(response.status)

    if response.status != "SUCCESS":
        raise RuntimeError(
            response.status
        )

    raw = response.items[0].data

    print()
    print("Raw analytics response:")
    print(raw)

    payload = json.loads(raw)

    products = payload.get(
        "products"
    )

    observations = payload.get(
        "observations"
    )

    actions = payload.get(
        "actions"
    )

    if not isinstance(products, list):
        raise RuntimeError(
            "Missing products."
        )

    if not isinstance(observations, list):
        raise RuntimeError(
            "Missing observations."
        )

    if not isinstance(actions, list):
        raise RuntimeError(
            "Missing actions."
        )

    print()
    print("========================================")
    print("INVENTORY OBSERVATIONS")
    print("========================================")

    for observation in observations:
        print(f"- {observation}")

    print()
    print("========================================")
    print("MANAGEMENT ACTIONS")
    print("========================================")

    for action in actions:
        print(f"- {action}")

    print()
    print("Products analyzed:", len(products))

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
    print("ANALYTICS INTEGRATION TEST SUCCESSFUL")
    print("=" * 40)


if __name__ == "__main__":
    main()