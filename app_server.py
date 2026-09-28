import json
import threading
import time
from concurrent import futures
import uuid

import grpc

import database
import inventory_pb2
import inventory_pb2_grpc


APP_SERVER_ADDRESS = "localhost:50052"

LLM_SERVER_ADDRESS = "localhost:50051"


# ============================================================
# CLIENT SERVICE
# ============================================================

class ClientServiceServicer(
    inventory_pb2_grpc.ClientServiceServicer
):

    def __init__(self):
        # Make sure the database schema exists.
        database.initialize_database()
        self.sessions = {}
        self.lock = threading.Lock()
        channel = grpc.insecure_channel(LLM_SERVER_ADDRESS)
        self.llm_stub = inventory_pb2_grpc.LLMServiceStub(channel)

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

                # ----------------------------------------------------
        # ORDER HISTORY
        # ----------------------------------------------------

        if request.type == "order_history":

            orders = database.get_order_history(
                user_id
            )

            items = []

            for order in orders:

                items.append(
                    inventory_pb2.DataItem(
                        id=str(order["id"]),
                        data=(
                            f"Order #{order['id']} | "
                            f"{order['product_name']} | "
                            f"Quantity: {order['quantity']} | "
                            f"Status: {order['status']} | "
                            f"{order['created_at']}"
                        ),
                    )
                )

            return inventory_pb2.GetResponse(
                status="SUCCESS",
                items=items,
            )

        if request.type == "llm_demand_prediction":
            try:
                forecasts = self._get_llm_demand_forecast()

                return inventory_pb2.GetResponse(
                    status="SUCCESS",
                    items=[
                        inventory_pb2.DataItem(
                            id="demand_forecast",
                            data=json.dumps(
                                {
                                    "forecasts": forecasts
                                },
                                indent=2,
                            ),
                        )
                    ],
                )

            except Exception as exc:
                print(
                    f"[Application Server] Demand forecast failed: {exc}"
                )

                return inventory_pb2.GetResponse(
                    status=f"LLM_ERROR: {exc}",
                    items=[],
                )

        if request.type == "llm_reorder_suggestion":
            try:

                recommendations = (
                    self._get_llm_reorder_suggestions()
                )

                return inventory_pb2.GetResponse(
                    status="SUCCESS",
                    items=[
                        inventory_pb2.DataItem(
                            id="reorder_suggestions",
                            data=json.dumps(
                                {
                                    "recommendations":
                                        recommendations
                                },
                                indent=2,
                            ),
                        )
                    ],
                )

            except Exception as exc:

                print(
                    f"[Application Server] "
                    f"Reorder suggestion failed: {exc}"
                )

                return inventory_pb2.GetResponse(
                    status=f"LLM_ERROR: {exc}",
                    items=[],
                )

        if request.type == "llm_analytics":
            try:

                analytics = (
                    self._get_llm_analytics()
                )

                return inventory_pb2.GetResponse(
                    status="SUCCESS",
                    items=[
                        inventory_pb2.DataItem(
                            id="inventory_analytics",
                            data=json.dumps(
                                analytics,
                                indent=2,
                            ),
                        )
                    ],
                )

            except Exception as exc:

                print(
                    "[Application Server] "
                    f"Analytics failed: {exc}"
                )

                return inventory_pb2.GetResponse(
                    status=f"LLM_ERROR: {exc}",
                    items=[],
                )

    # --------------------------------------------------------
    # POST
    # --------------------------------------------------------

    def post(self, request, context):

        # --------------------------------------------------------
        # 1. Authentication
        # --------------------------------------------------------

        user_id = database.get_user_id_from_token(
            request.token
        )

        if user_id is None:
            return inventory_pb2.StatusResponse(
                status="UNAUTHORIZED",
                message="Valid login required.",
            )

        # --------------------------------------------------------
        # 2. Determine operation
        # --------------------------------------------------------

        if request.type != "order":
            return inventory_pb2.StatusResponse(
                status="UNKNOWN_OPERATION",
                message=(
                    f"Unsupported POST type: {request.type}"
                ),
            )

        # --------------------------------------------------------
        # 3. Parse JSON payload
        # --------------------------------------------------------

        try:
            import json

            order_data = json.loads(request.data)

            item_id = order_data["item_id"]
            quantity = int(order_data["quantity"])

        except (json.JSONDecodeError, KeyError, TypeError, ValueError):

            return inventory_pb2.StatusResponse(
                status="INVALID_DATA",
                message=(
                    "Order data must be JSON containing "
                    "'item_id' and 'quantity'."
                ),
            )

        # --------------------------------------------------------
        # 4. Execute transaction
        # --------------------------------------------------------

        try:

            result = database.place_order(
                user_id=user_id,
                item_id=item_id,
                quantity=quantity,
            )

        except Exception as exc:

            print(
                f"[ORDER] Database error: {exc}"
            )

            return inventory_pb2.StatusResponse(
                status="INTERNAL_ERROR",
                message="Unable to process order.",
            )

        # --------------------------------------------------------
        # 5. Return result
        # --------------------------------------------------------

        print(
            f"[ORDER] user={user_id} "
            f"item={item_id} "
            f"quantity={quantity} "
            f"status={result['status']}"
        )

        return inventory_pb2.StatusResponse(
            status=result["status"],
            message=result["message"],
        )

    def _get_llm_demand_forecast(self):
        """
        Fetch inventory + historical sales from SQLite,
        send them to the separate LLM server in batches,
        and validate the returned forecasts.
        """

        products = database.get_products_with_history()

        if not products:
            raise ValueError("No products found in the database.")

        all_forecasts = []

        # Send four products per LLM request.
        # This keeps the prompt manageable for the local Qwen3 model.
        batch_size = 4

        for start in range(0, len(products), batch_size):
            batch = products[start:start + batch_size]

            prepared_products = []

            for product in batch:
                history = product["sales_history"]

                if not history:
                    raise ValueError(
                        f"No sales history found for {product['item_id']}"
                    )

                # Descriptive statistics.
                # These are NOT the forecast.
                recent_7 = history[-7:]

                prepared_products.append(
                    {
                        "item_id": product["item_id"],
                        "product_name": product["product_name"],
                        "current_stock": product["current_stock"],
                        "sales_history": history,
                        "historical_30_day_total": sum(history),
                        "historical_30_day_average": round(
                            sum(history) / len(history), 2
                        ),
                        "recent_7_day_total": sum(recent_7),
                        "recent_7_day_average": round(
                            sum(recent_7) / len(recent_7), 2
                        ),
                        "minimum_daily_sales": min(history),
                        "maximum_daily_sales": max(history),
                    }
                )

            # The LLM service accepts a JSON array of products.  Sending an
            # object such as {"products": [...]} makes the LLM server iterate
            # over the string key "products" rather than the product records.
            context_data = json.dumps(
                prepared_products,
                indent=2,
            )

            request_id = str(uuid.uuid4())

            print(
                f"[LLM Routing] Demand prediction batch "
                f"{start + 1}-{min(start + batch_size, len(products))} "
                f"-> Node 1 | request_id={request_id}"
            )

            response = self.llm_stub.getLLMAnswer(
                inventory_pb2.LLMRequest(
                    request_id=request_id,
                    query="llm_demand_prediction",
                    context=context_data,
                ),
                timeout=180,
            )

            try:
                result = json.loads(response.answer)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"LLM returned invalid JSON: {response.answer}"
                ) from exc

            forecasts = result.get("forecasts")

            if not isinstance(forecasts, list):
                raise ValueError(
                    "LLM response is missing the 'forecasts' array."
                )

            expected_ids = {
                product["item_id"]
                for product in prepared_products
            }

            returned_ids = set()

            for forecast in forecasts:
                item_id = forecast.get("item_id")
                predicted = forecast.get("predicted_7_day_demand")

                if item_id not in expected_ids:
                    raise ValueError(
                        f"LLM returned unknown item: {item_id}"
                    )

                if item_id in returned_ids:
                    raise ValueError(
                        f"LLM returned duplicate forecast for: {item_id}"
                    )

                if (
                    not isinstance(predicted, int)
                    or isinstance(predicted, bool)
                    or predicted < 0
                ):
                    raise ValueError(
                        f"Invalid forecast for {item_id}: {predicted!r}"
                    )

                returned_ids.add(item_id)

                all_forecasts.append(
                    {
                        "item_id": item_id,
                        "product_name": str(
                            forecast.get("product_name", "")
                        ),
                        "predicted_7_day_demand": predicted,
                        "trend": str(
                            forecast.get("trend", "unknown")
                        ),
                        "reason": str(
                            forecast.get("reason", "")
                        ),
                    }
                )

            if returned_ids != expected_ids:
                missing = expected_ids - returned_ids

                raise ValueError(
                    "LLM did not return forecasts for: "
                    + ", ".join(sorted(missing))
                )

        # Return only after every batch has been requested.  Returning inside
        # the loop previously discarded all but the first four forecasts.
        return all_forecasts


    def _get_llm_reorder_suggestions(self):
        """
        Get the validated demand forecast, then ask the LLM
        whether each item needs to be reordered.
        """

        forecasts = self._get_llm_demand_forecast()

        products = database.get_all_products()

        stock_by_item = {
            product["item_id"]: product
            for product in products
        }

        reorder_input = []

        for forecast in forecasts:

            item_id = forecast["item_id"]

            if item_id not in stock_by_item:
                raise ValueError(
                    f"Product {item_id} not found in database."
                )

            current_stock = stock_by_item[item_id]["stock"]

            reorder_input.append({
                "item_id": item_id,
                "product_name": forecast["product_name"],
                "current_stock": current_stock,
                "predicted_7_day_demand": (
                    forecast["predicted_7_day_demand"]
                ),
            })

        context_data = json.dumps(
            reorder_input,
            indent=2,
        )

        request_id = str(uuid.uuid4())

        print(
            f"[LLM Routing] Reorder suggestion "
            f"-> Node 1 | request_id={request_id}"
        )

        response = self.llm_stub.getLLMAnswer(
            inventory_pb2.LLMRequest(
                request_id=request_id,
                query="llm_reorder_suggestion",
                context=context_data,
            ),
            timeout=180,
        )

        try:
            result = json.loads(response.answer)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"LLM returned invalid JSON: {response.answer}"
            ) from exc

        recommendations = result.get("recommendations")

        if not isinstance(recommendations, list):
            raise ValueError(
                "LLM response is missing the "
                "'recommendations' array."
            )

        expected_ids = {
            product["item_id"]
            for product in reorder_input
        }

        returned_ids = set()
        final_recommendations = []

        for recommendation in recommendations:

            item_id = recommendation.get("item_id")

            if item_id not in expected_ids:
                raise ValueError(
                    f"LLM returned unknown item: {item_id}"
                )

            if item_id in returned_ids:
                raise ValueError(
                    f"Duplicate reorder recommendation: {item_id}"
                )

            reorder = recommendation.get("reorder")

            if not isinstance(reorder, bool):
                raise ValueError(
                    f"Invalid reorder value for {item_id}."
                )

            product = stock_by_item[item_id]

            forecast = next(
                item
                for item in forecasts
                if item["item_id"] == item_id
            )

            current_stock = product["stock"]
            predicted_demand = (
                forecast["predicted_7_day_demand"]
            )

            # Application server performs the arithmetic.
            reorder_quantity = max(
                predicted_demand - current_stock,
                0,
            )

            # Make the final recommendation internally consistent.
            expected_reorder = reorder_quantity > 0

            final_recommendations.append({
                "item_id": item_id,
                "product_name": product["name"],
                "current_stock": current_stock,
                "predicted_7_day_demand": predicted_demand,
                "reorder": expected_reorder,
                "reorder_quantity": reorder_quantity,
                "reason": str(
                    recommendation.get("reason", "")
                ),
            })

            returned_ids.add(item_id)

        if returned_ids != expected_ids:

            missing = expected_ids - returned_ids

            raise ValueError(
                "LLM did not return reorder decisions for: "
                + ", ".join(sorted(missing))
            )

        return final_recommendations


    def _get_llm_analytics(self):
        """
        Build a validated inventory snapshot using:
        - current database stock
        - LLM demand forecast
        - LLM reorder decisions

        Then ask Node 1 to summarize the overall inventory situation.
        """

        forecasts = self._get_llm_demand_forecast()

        reorder_results = self._get_llm_reorder_suggestions()

        products = database.get_all_products()

        stock_by_item = {
            product["item_id"]: product
            for product in products
        }

        reorder_by_item = {
            item["item_id"]: item
            for item in reorder_results
        }

        analytics_products = []

        for forecast in forecasts:

            item_id = forecast["item_id"]

            if item_id not in stock_by_item:
                raise ValueError(
                    f"Product {item_id} not found in database."
                )

            if item_id not in reorder_by_item:
                raise ValueError(
                    f"Missing reorder result for {item_id}."
                )

            product = stock_by_item[item_id]
            reorder = reorder_by_item[item_id]

            current_stock = product["stock"]
            predicted_demand = (
                forecast["predicted_7_day_demand"]
            )

            stock_status = (
                "LOW"
                if current_stock < predicted_demand
                else "ADEQUATE"
            )

            analytics_products.append(
                {
                    "item_id": item_id,
                    "product_name": product["name"],
                    "current_stock": current_stock,
                    "predicted_7_day_demand": predicted_demand,
                    "trend": forecast["trend"],
                    "reorder": reorder["reorder"],
                    "reorder_quantity": reorder[
                        "reorder_quantity"
                    ],
                    "stock_status": stock_status,
                }
            )

        analytics_context = {
            "products": analytics_products
        }

        context_data = json.dumps(
            analytics_context,
            indent=2
        )

        request_id = str(uuid.uuid4())

        print(
            f"[LLM Routing] Analytics "
            f"-> Node 1 | request_id={request_id}"
        )

        response = self.llm_stub.getLLMAnswer(
            inventory_pb2.LLMRequest(
                request_id=request_id,
                query="llm_analytics",
                context=context_data,
            ),
            timeout=180,
        )

        try:

            result = json.loads(
                response.answer
            )

        except json.JSONDecodeError as exc:

            raise ValueError(
                f"LLM returned invalid JSON: "
                f"{response.answer}"
            ) from exc

        observations = result.get(
            "observations"
        )

        actions = result.get(
            "actions"
        )

        if not isinstance(observations, list):
            raise ValueError(
                "LLM analytics response is missing "
                "'observations'."
            )

        if not isinstance(actions, list):
            raise ValueError(
                "LLM analytics response is missing "
                "'actions'."
            )

        return {
            "products": analytics_products,
            "observations": observations,
            "actions": actions,
        }



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
