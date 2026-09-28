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

    print("=== LOGIN TEST ===")

    login_response = stub.login(
        inventory_pb2.LoginRequest(
            username="alice",
            password="password",
        )
    )

    print("Status:", login_response.status)

    if not login_response.token:
        print("Login failed.")
        return

    token = login_response.token

    print("Token received:", token[:12] + "...")

    print()
    print("=== INVENTORY TEST ===")

    inventory_response = stub.get(
        inventory_pb2.GetRequest(
            token=token,
            type="inventory",
        )
    )

    print(
        "Inventory status:",
        inventory_response.status,
    )

    for item in inventory_response.items:

        print(
            item.id,
            "->",
            item.data,
        )

    print()
    print("=== LOGOUT TEST ===")

    logout_response = stub.logout(
        inventory_pb2.LogoutRequest(
            token=token,
        )
    )

    print(
        "Logout status:",
        logout_response.status,
    )


if __name__ == "__main__":
    main()