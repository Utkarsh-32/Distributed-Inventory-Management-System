import json

import grpc

import inventory_pb2
import inventory_pb2_grpc


APP_SERVER_ADDRESS = "localhost:50052"


def main():
    print("========================================")
    print("APPLICATION → LLM DEMAND TEST")
    print("========================================")

    channel = grpc.insecure_channel(APP_SERVER_ADDRESS)
    stub = inventory_pb2_grpc.ClientServiceStub(channel)

    # Login
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

    # Ask Application Server for demand prediction
    response = stub.get(
        inventory_pb2.GetRequest(
            token=token,
            type="llm_demand_prediction",
        ),
        timeout=240,
    )

    print()
    print("Application Server status:")
    print(response.status)

    if response.status != "SUCCESS":
        raise RuntimeError(response.status)

    if not response.items:
        raise RuntimeError(
            "Application Server returned no forecast data."
        )

    raw = response.items[0].data

    print()
    print("Raw forecast response:")
    print(raw)

    result = json.loads(raw)

    forecasts = result.get("forecasts")

    if not isinstance(forecasts, list):
        raise RuntimeError(
            "Forecast response does not contain a list."
        )

    if len(forecasts) != 12:
        raise RuntimeError(
            f"Expected 12 forecasts, got {len(forecasts)}"
        )

    seen_ids = set()

    for forecast in forecasts:
        item_id = forecast["item_id"]
        product_name = forecast["product_name"]
        predicted = forecast["predicted_7_day_demand"]
        trend = forecast["trend"]
        reason = forecast["reason"]

        if item_id in seen_ids:
            raise RuntimeError(
                f"Duplicate forecast: {item_id}"
            )

        seen_ids.add(item_id)

        if not isinstance(predicted, int) or isinstance(predicted, bool):
            raise RuntimeError(
                f"Invalid prediction for {item_id}: {predicted!r}"
            )

        if predicted < 0:
            raise RuntimeError(
                f"Negative prediction for {item_id}"
            )

        print(
            f"\n{product_name} ({item_id})"
        )
        print(
            f"Predicted next 7 days: {predicted} units"
        )
        print(f"Trend: {trend}")
        print(f"Reason: {reason}")

    # Logout
    logout_response = stub.logout(
        inventory_pb2.LogoutRequest(token=token)
    )

    print()
    print(f"Logout: {logout_response.status}")

    print()
    print("========================================")
    print("DEMAND INTEGRATION TEST SUCCESSFUL")
    print("========================================")


if __name__ == "__main__":
    main()