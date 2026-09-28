import time
from concurrent import futures

import grpc

import database
import inventory_pb2
import inventory_pb2_grpc


APP_SERVER_ADDRESS = "localhost:50052"


# ============================================================
# CLIENT SERVICE
# ============================================================

class ClientServiceServicer(
    inventory_pb2_grpc.ClientServiceServicer
):

    def __init__(self):
        # Make sure the database schema exists.
        database.initialize_database()

    # --------------------------------------------------------
    # LOGIN
    # --------------------------------------------------------

    def login(self, request, context):

        token = database.authenticate_user(
            request.username,
            request.password,
        )

        if token is None:
            print(
                f"[AUTH] Failed login attempt: "
                f"{request.username}"
            )

            return inventory_pb2.LoginResponse(
                status="FAILED_INVALID_CREDENTIALS",
                token="",
            )

        print(
            f"[AUTH] Successful login: "
            f"{request.username}"
        )

        return inventory_pb2.LoginResponse(
            status="SUCCESS",
            token=token,
        )

    # --------------------------------------------------------
    # LOGOUT
    # --------------------------------------------------------

    def logout(self, request, context):

        success = database.logout_user(
            request.token
        )

        if success:
            print("[AUTH] Logout successful")

            return inventory_pb2.StatusResponse(
                status="SUCCESS",
                message="Logged out successfully",
            )

        return inventory_pb2.StatusResponse(
            status="INVALID_TOKEN",
            message="Session token is invalid",
        )

    # --------------------------------------------------------
    # GET
    # --------------------------------------------------------

    def get(self, request, context):

        user_id = database.get_user_id_from_token(
            request.token
        )

        # Every protected GET operation requires
        # authentication.
        if user_id is None:

            return inventory_pb2.GetResponse(
                status="UNAUTHORIZED",
                items=[],
            )

        # ----------------------------------------------
        # INVENTORY
        # ----------------------------------------------

        if request.type == "inventory":

            products = database.get_all_products()

            items = []

            for product in products:

                items.append(
                    inventory_pb2.DataItem(
                        id=product["item_id"],
                        data=(
                            f"Name: {product['name']} | "
                            f"Stock: {product['stock']}"
                        ),
                    )
                )

            return inventory_pb2.GetResponse(
                status="SUCCESS",
                items=items,
            )

        return inventory_pb2.GetResponse(
            status="UNKNOWN_TYPE",
            items=[],
        )

    # --------------------------------------------------------
    # POST
    # --------------------------------------------------------

    def post(self, request, context):

        # We haven't implemented orders yet.
        # That's intentionally coming in the next layer.

        user_id = database.get_user_id_from_token(
            request.token
        )

        if user_id is None:

            return inventory_pb2.StatusResponse(
                status="UNAUTHORIZED",
                message="Valid login required",
            )

        return inventory_pb2.StatusResponse(
            status="NOT_IMPLEMENTED",
            message=(
                "POST operations will be added "
                "in the order-processing phase."
            ),
        )


# ============================================================
# INTERNAL APPLICATION SERVER SERVICE
# ============================================================

class AppServerServiceServicer(
    inventory_pb2_grpc.AppServerServiceServicer
):

    def processBusinessRequest(
        self,
        request,
        context,
    ):

        print(
            f"[APP] Business request: "
            f"{request.request_id}"
        )

        return inventory_pb2.StatusResponse(
            status="BUSINESS_REQUEST_RECEIVED",
            message="Request received",
        )


# ============================================================
# SERVER
# ============================================================

def serve():

    server = grpc.server(
        futures.ThreadPoolExecutor(
            max_workers=10
        )
    )

    inventory_pb2_grpc.add_ClientServiceServicer_to_server(
        ClientServiceServicer(),
        server,
    )

    inventory_pb2_grpc.add_AppServerServiceServicer_to_server(
        AppServerServiceServicer(),
        server,
    )

    server.add_insecure_port(
        APP_SERVER_ADDRESS
    )

    server.start()

    print("========================================")
    print("Application Server - Node 2")
    print("========================================")
    print(
        f"Listening on: {APP_SERVER_ADDRESS}"
    )
    print("Database: inventory.db")
    print("========================================")

    try:

        while True:
            time.sleep(24 * 60 * 60)

    except KeyboardInterrupt:

        print("\nStopping Application Server...")
        server.stop(0)


if __name__ == "__main__":
    serve()