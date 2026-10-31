import json
import uuid

import grpc

import inventory_pb2
import inventory_pb2_grpc


def get_stock(stub, token, item_id):
    response = stub.get(
        inventory_pb2.GetRequest(
            token=token,
            type="inventory",
        )
    )

    for item in response.items:
        if item.id == item_id:
            return int(item.data.rsplit("Stock: ", 1)[1])

    raise RuntimeError("Item not found.")


channel = grpc.insecure_channel("localhost:50052")
stub = inventory_pb2_grpc.ClientServiceStub(channel)

login = stub.login(
    inventory_pb2.LoginRequest(
        username="alice",
        password="password",
    )
)

token = login.token
item_id = "item_102"
quantity = 1

stock_before = get_stock(stub, token, item_id)

# Make ONE key and reuse it.
same_key = str(uuid.uuid4())

request = inventory_pb2.PostRequest(
    token=token,
    type="order",
    data=json.dumps(
        {
            "item_id": item_id,
            "quantity": quantity,
        }
    ),
    idempotency_key=same_key,
)

first_response = stub.post(request)
retry_response = stub.post(request)

stock_after = get_stock(stub, token, item_id)

print("First request:", first_response.status, first_response.message)
print("Retry:", retry_response.status, retry_response.message)
print("Stock before:", stock_before)
print("Stock after:", stock_after)

assert first_response.status == "SUCCESS"
assert retry_response.status == "SUCCESS"
assert stock_after == stock_before - quantity

print("Idempotency test passed.")
