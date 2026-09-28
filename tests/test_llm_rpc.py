import json
import uuid

import grpc

import inventory_pb2
import inventory_pb2_grpc


LLM_SERVER_ADDRESS = "localhost:50051"


def main():

    # --------------------------------------------------------
    # Sample inventory context
    # --------------------------------------------------------

    products = [
        {
            "item_id": "item_101",
            "product_name": "Laptop",
            "current_stock": 50,
            "sales_history": [
                4, 5, 4, 6, 5,
                7, 5, 6, 7, 8,
                7, 8, 9, 8, 9,
                10, 8, 9, 10, 11,
                9, 10, 11, 10, 12,
                11, 12, 13, 12, 14
            ],
            "last_7_day_total": 84,
            "last_7_day_average": 12.0,
            "30_day_total": 260,
            "30_day_average": 8.67,
            "trend_slope": 0.295,
        },
        {
            "item_id": "item_102",
            "product_name": "Wireless Mouse",
            "current_stock": 20,
            "sales_history": [
                4, 5, 4, 6, 5,
                7, 5, 6, 7, 8,
                7, 8, 9, 8, 9,
                10, 8, 9, 10, 11,
                9, 10, 11, 10, 12,
                11, 12, 13, 12, 14
            ],
            "last_7_day_total": 84,
            "last_7_day_average": 12.0,
            "30_day_total": 260,
            "30_day_average": 8.67,
            "trend_slope": 0.295,
        },
    ]

    # --------------------------------------------------------
    # Connect to LLM Server
    # --------------------------------------------------------

    channel = grpc.insecure_channel(
        LLM_SERVER_ADDRESS
    )

    stub = inventory_pb2_grpc.LLMServiceStub(
        channel
    )

    # --------------------------------------------------------
    # Send request
    # --------------------------------------------------------

    request_id = str(
        uuid.uuid4()
    )

    response = stub.getLLMAnswer(
        inventory_pb2.LLMRequest(
            request_id=request_id,
            query="llm_demand_prediction",
            context=json.dumps(products),
        ),
        timeout=120,
    )

    # --------------------------------------------------------
    # Display response
    # --------------------------------------------------------

    print("========================================")
    print("LLM RPC TEST")
    print("========================================")

    print(
        "Request ID:",
        response.request_id,
    )

    print()
    print("Raw response:")
    print(response.answer)

    print()
    print("Parsed JSON:")

    result = json.loads(
        response.answer
    )

    for forecast in result["forecasts"]:

        print(
            f"{forecast['product_name']}: "
            f"{forecast['predicted_7_day_demand']} "
            f"units"
        )

    print()
    print("LLM RPC test successful.")


if __name__ == "__main__":
    main()