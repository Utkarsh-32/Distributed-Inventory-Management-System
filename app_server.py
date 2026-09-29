import json
import os
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
# Qualitative insight requests are deliberately short, but generating a
# structured forecast for the complete inventory can take much longer on a
# locally hosted model (and the first request may need to load the model).
LLM_REQUEST_TIMEOUT_SECONDS = 20
LLM_FORECAST_REQUEST_TIMEOUT_SECONDS = 300
DASHBOARD_CACHE_TTL_SECONDS = 300
USE_LLM_FORECASTS = os.getenv(
    "USE_LLM_FORECASTS",
    "false",
).lower() in {"1", "true", "yes"}


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
        self.dashboard_lock = threading.Lock()
        self.dashboard_cache = None
        self.dashboard_cache_created_at = 0.0
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
                    self._get_llm_inventory_analytics()
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

        if request.type == "llm_dashboard":
            try:
                dashboard = self._get_cached_llm_dashboard()

                return inventory_pb2.GetResponse(
                    status="SUCCESS",
                    items=[
                        inventory_pb2.DataItem(
                            id="llm_dashboard",
                            data=json.dumps(
                                dashboard,
                                indent=2
                            )
                        )
                    ]
                )

            except Exception as exc:
                print(
                    "[Application Server] "
                    f"Dashboard LLM request failed: {exc}"
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

        if result["success"]:
            self._invalidate_dashboard_cache()

        return inventory_pb2.StatusResponse(
            status=result["status"],
            message=result["message"],
        )

    def _get_llm_demand_forecast(self):
        """
        Fetch inventory and historical sales from SQLite. By default the
        dashboard uses a fast deterministic forecast; setting
        USE_LLM_FORECASTS=true restores model-generated forecasts.
        """
        products = database.get_products_with_history()

        if not products:
            raise ValueError("No products found in the database.")

        if not USE_LLM_FORECASTS:
            return self._get_fast_demand_forecast(products)

        all_forecasts = []

        # One compact request avoids repeatedly loading and prompting the
        # local model.  The model receives summary statistics rather than
        # all 30 daily values, which is enough for a 7-day forecast.
        batch_size = len(products)

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
                        "recent_7_day_sales": recent_7,
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
                timeout=LLM_FORECAST_REQUEST_TIMEOUT_SECONDS,
            )

            try:
                result = json.loads(response.answer)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"LLM returned invalid JSON: {response.answer}"
                ) from exc

            if not isinstance(result, dict):
                raise ValueError(
                    "LLM forecast response is not a JSON object."
                )

            if "error" in result:
                raise ValueError(
                    "LLM forecast failed: "
                    + str(result["error"])
                )

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

    def _get_fast_demand_forecast(self, products):
        """Forecast from recent and historical sales without model latency."""
        forecasts = []

        for product in products:
            history = product["sales_history"]

            if not history:
                raise ValueError(
                    f"No sales history found for {product['item_id']}"
                )

            historical_average = sum(history) / len(history)
            recent_average = sum(history[-7:]) / min(7, len(history))
            predicted_demand = max(
                0,
                round((recent_average * 0.7 + historical_average * 0.3) * 7),
            )

            if recent_average > historical_average * 1.10:
                trend = "increasing"
            elif recent_average < historical_average * 0.90:
                trend = "decreasing"
            else:
                trend = "stable"

            forecasts.append(
                {
                    "item_id": product["item_id"],
                    "product_name": product["product_name"],
                    "predicted_7_day_demand": predicted_demand,
                    "trend": trend,
                    "reason": (
                        "Weighted 7-day forecast from recent and "
                        "30-day sales averages."
                    ),
                }
            )

        return forecasts

    def _build_inventory_facts(self, forecasts):
        """
        Build authoritative inventory facts from the current database
        state and the already-computed demand forecasts.

        This method is shared by reorder suggestions and analytics so
        both features use the same forecast values and deterministic
        business rules.
        """

        if not forecasts:
            raise ValueError(
                "No demand forecasts available."
            )

        products = database.get_products_with_history()

        if not products:
            raise ValueError(
                "No products found in the database."
            )

        forecast_by_id = {
            forecast["item_id"]: forecast
            for forecast in forecasts
        }

        inventory_facts = []

        for product in products:

            item_id = product["item_id"]

            if item_id not in forecast_by_id:
                raise ValueError(
                    f"Missing forecast for {item_id}"
                )

            forecast = forecast_by_id[item_id]

            history = product["sales_history"]

            if not history:
                raise ValueError(
                    f"No sales history for {item_id}"
                )

            recent_7 = history[-7:]

            historical_average = round(
                sum(history) / len(history),
                2,
            )

            recent_average = round(
                sum(recent_7) / len(recent_7),
                2,
            )

            current_stock = int(
                product["current_stock"]
            )

            predicted_demand = int(
                forecast["predicted_7_day_demand"]
            )

            # Authoritative reorder rule.
            reorder = (
                predicted_demand > current_stock
            )

            reorder_quantity = max(
                predicted_demand - current_stock,
                0,
            )

            stock_status = (
                "LOW"
                if reorder
                else "ADEQUATE"
            )

            # Authoritative demand direction.
            if recent_average > historical_average * 1.10:
                demand_direction = "INCREASING"
            elif recent_average < historical_average * 0.90:
                demand_direction = "DECREASING"
            else:
                demand_direction = "STABLE"

            inventory_facts.append(
                {
                    "item_id": item_id,
                    "product_name": product["product_name"],
                    "current_stock": current_stock,
                    "predicted_7_day_demand": predicted_demand,
                    "historical_30_day_average": historical_average,
                    "recent_7_day_average": recent_average,
                    "demand_direction": demand_direction,
                    "trend": forecast.get(
                        "trend",
                        "unknown",
                    ),
                    "reason": forecast.get(
                        "reason",
                        "",
                    ),
                    "reorder": reorder,
                    "reorder_quantity": reorder_quantity,
                    "stock_status": stock_status,
                }
            )

        return inventory_facts

    def _get_llm_reorder_suggestions(self, forecasts = None):
        """
        Generate deterministic reorder recommendations from the validated
        forecast.  A previous version made an additional LLM request only
        to restate this same calculation, which made the dashboard slower
        without changing the recommendation.
        """

        # First obtain the authoritative demand forecasts.
        if forecasts is None:
            forecasts = self._get_llm_demand_forecast()

        # Build deterministic inventory facts.
        inventory_facts = self._build_inventory_facts(
            forecasts
        )

        final_recommendations = []

        for item in inventory_facts:

            item_id = item["item_id"]

            final_recommendations.append(
                {
                    "item_id": item_id,
                    "product_name": item["product_name"],

                    # ALWAYS use Python calculation.
                    "reorder": item["reorder"],
                    "reorder_quantity": item[
                        "reorder_quantity"
                    ],

                    "current_stock": item[
                        "current_stock"
                    ],

                    "predicted_7_day_demand": item[
                        "predicted_7_day_demand"
                    ],

                    "stock_status": item[
                        "stock_status"
                    ],

                    "trend": item["trend"],

                    "reason": (
                        f"{item['current_stock']} units in stock versus "
                        f"{item['predicted_7_day_demand']} units expected "
                        "over the next 7 days."
                    ),
                }
            )

        return final_recommendations

    def _get_llm_inventory_analytics(self, forecasts = None):
        """
        Generate inventory analytics.

        Python owns all deterministic calculations:
        - reorder
        - reorder_quantity
        - stock_status
        - demand_direction

        The LLM provides qualitative insights only.
        """

        # ----------------------------------------------------
        # 1. Get demand forecasts
        # ----------------------------------------------------
        if forecasts is None:
            forecasts = self._get_llm_demand_forecast()

        if not forecasts:
            raise ValueError(
                "No demand forecasts available for analytics."
            )

        # ----------------------------------------------------
        # 2. Build authoritative product facts
        # ----------------------------------------------------

        products = database.get_products_with_history()

        if not products:
            raise ValueError(
                "No products found in the database."
            )

        forecast_by_id = {
            forecast["item_id"]: forecast
            for forecast in forecasts
        }

        inventory_facts = []

        for product in products:

            item_id = product["item_id"]

            if item_id not in forecast_by_id:
                raise ValueError(
                    f"Missing forecast for {item_id}"
                )

            forecast = forecast_by_id[item_id]

            history = product["sales_history"]

            if not history:
                raise ValueError(
                    f"No sales history for {item_id}"
                )

            recent_7 = history[-7:]

            # ------------------------------------------------
            # Authoritative numerical statistics
            # ------------------------------------------------

            historical_average = round(
                sum(history) / len(history),
                2,
            )

            recent_average = round(
                sum(recent_7) / len(recent_7),
                2,
            )

            current_stock = int(
                product["current_stock"]
            )

            predicted_demand = int(
                forecast["predicted_7_day_demand"]
            )

            # ------------------------------------------------
            # Authoritative business rules
            # ------------------------------------------------

            reorder = (
                predicted_demand > current_stock
            )

            reorder_quantity = max(
                predicted_demand - current_stock,
                0,
            )

            stock_status = (
                "LOW"
                if reorder
                else "ADEQUATE"
            )

            # ------------------------------------------------
            # Authoritative demand classification
            # ------------------------------------------------
            demand_direction = "STABLE"
            if recent_average > historical_average * 1.10:

                demand_direction = "INCREASING"

            elif recent_average < historical_average * 0.90:

                demand_direction = "DECREASING"

            else:

                demand_direction = "STABLE"

            inventory_facts.append(
                {
                    "item_id": item_id,
                    "product_name": product["product_name"],

                    "current_stock": current_stock,

                    "predicted_7_day_demand":
                        predicted_demand,

                    "historical_30_day_average":
                        historical_average,

                    "recent_7_day_average":
                        recent_average,

                    "demand_direction":
                        demand_direction,

                    "trend":
                        forecast.get(
                            "trend",
                            "unknown",
                        ),

                    "reason":
                        forecast.get(
                            "reason",
                            "",
                        ),

                    "reorder":
                        reorder,

                    "reorder_quantity":
                        reorder_quantity,

                    "stock_status":
                        stock_status,
                }
            )

        # ----------------------------------------------------
        # 3. Send authoritative facts to LLM
        # ----------------------------------------------------

        llm_context = {
            "products": inventory_facts,

            "instructions": {
                "role":
                    "inventory analytics assistant",

                "important":
                    (
                        "The numerical and inventory fields "
                        "provided for each product are authoritative."
                    ),

                "do_not_modify": [
                    "current_stock",
                    "predicted_7_day_demand",
                    "historical_30_day_average",
                    "recent_7_day_average",
                    "demand_direction",
                    "reorder",
                    "reorder_quantity",
                    "stock_status",
                ],

                "task":
                    (
                        "Generate concise qualitative observations "
                        "about demand patterns. Do not change or "
                        "reinterpret the supplied inventory facts."
                    ),
            },
        }

        context_data = json.dumps(
            llm_context,
            indent=2,
        )

        request_id = str(uuid.uuid4())

        print(
            "[LLM Routing] Inventory analytics "
            "-> Node 1 | "
            f"request_id={request_id}"
        )

        # ----------------------------------------------------
        # 4. Ask LLM for qualitative insights
        # ----------------------------------------------------

        llm_insights = []
        llm_error = None

        try:
            response = self.llm_stub.getLLMAnswer(
                inventory_pb2.LLMRequest(
                    request_id=request_id,
                    query="llm_analytics",
                    context=context_data,
                ),
                timeout=LLM_REQUEST_TIMEOUT_SECONDS,
            )
            llm_result = json.loads(response.answer)

            if "error" in llm_result:
                raise ValueError(str(llm_result["error"]))

            response_insights = llm_result.get("observations", [])
            if not isinstance(response_insights, list) or not all(
                isinstance(insight, str)
                for insight in response_insights
            ):
                raise ValueError("LLM analytics response has invalid insights.")

            llm_insights = response_insights

        except (grpc.RpcError, ValueError, json.JSONDecodeError) as exc:
            # Numerical analytics are calculated above, so an unavailable
            # local model must not make the entire dashboard unusable.
            llm_error = str(exc)
            print(f"[LLM] Optional analytics insight unavailable: {exc}")

        # ----------------------------------------------------
        # 5. Create authoritative observations
        # ----------------------------------------------------

        reorder_items = [
            item
            for item in inventory_facts
            if item["reorder"]
        ]

        increasing_items = [
            item
            for item in inventory_facts
            if item["demand_direction"]
            == "INCREASING"
        ]

        decreasing_items = [
            item
            for item in inventory_facts
            if item["demand_direction"]
            == "DECREASING"
        ]

        stable_items = [
            item
            for item in inventory_facts
            if item["demand_direction"]
            == "STABLE"
        ]

        observations = []

        observations.append(
            (
                f"{len(reorder_items)} of "
                f"{len(inventory_facts)} products "
                "require replenishment based on "
                "predicted 7-day demand."
            )
        )

        if reorder_items:

            names = ", ".join(
                item["product_name"]
                for item in reorder_items
            )

            observations.append(
                "Products requiring reorder: "
                + names
                + "."
            )

        if increasing_items:

            names = ", ".join(
                item["product_name"]
                for item in increasing_items
            )

            observations.append(
                "Increasing demand detected for: "
                + names
                + "."
            )

        if decreasing_items:

            names = ", ".join(
                item["product_name"]
                for item in decreasing_items
            )

            observations.append(
                "Decreasing demand detected for: "
                + names
                + "."
            )

        if stable_items:

            names = ", ".join(
                item["product_name"]
                for item in stable_items
            )

            observations.append(
                "Stable demand detected for: "
                + names
                + "."
            )

        # ----------------------------------------------------
        # 6. Create authoritative management actions
        # ----------------------------------------------------

        actions = []

        for item in reorder_items:

            actions.append(
                (
                    f"Reorder '{item['product_name']}' "
                    f"by {item['reorder_quantity']} units "
                    "to cover the predicted 7-day demand."
                )
            )

        for item in increasing_items:

            if item["reorder"]:

                actions.append(
                    (
                        f"Monitor '{item['product_name']}' "
                        "closely because demand is increasing "
                        "and current stock is below predicted demand."
                    )
                )

            else:

                actions.append(
                    (
                        f"Monitor '{item['product_name']}' "
                        "because demand is increasing, "
                        "although current stock is sufficient "
                        "for predicted 7-day demand."
                    )
                )

        for item in decreasing_items:

            actions.append(
                (
                    f"Review '{item['product_name']}' "
                    "because recent demand is decreasing."
                )
            )

        # ----------------------------------------------------
        # 7. Final response
        # ----------------------------------------------------

        return {
            "products": inventory_facts,
            "observations": observations,
            "actions": actions,
            "llm_insights": llm_insights,
            "llm_error": llm_error,
        }

    def _get_llm_dashboard(self):

        # Generate demand forecast ONLY ONCE
        forecasts = self._get_llm_demand_forecast()

        # Reuse the same forecasts
        reorder_recommendations = self._get_llm_reorder_suggestions(
            forecasts=forecasts
        )

        # Reuse the same forecasts again
        analytics = self._get_llm_inventory_analytics(
            forecasts=forecasts
        )

        return {
            "forecasts": forecasts,
            "reorder_recommendations": reorder_recommendations,
            "analytics": analytics,
        }

    def _get_cached_llm_dashboard(self):
        """Return recent dashboard results without recomputing the model."""
        with self.dashboard_lock:
            cache_is_fresh = (
                self.dashboard_cache is not None
                and time.monotonic() - self.dashboard_cache_created_at
                < DASHBOARD_CACHE_TTL_SECONDS
            )

            if cache_is_fresh:
                return self.dashboard_cache

            dashboard = self._get_llm_dashboard()
            self.dashboard_cache = dashboard
            self.dashboard_cache_created_at = time.monotonic()
            return dashboard

    def _invalidate_dashboard_cache(self):
        """Ensure the next dashboard run reflects a successful order."""
        with self.dashboard_lock:
            self.dashboard_cache = None
            self.dashboard_cache_created_at = 0.0



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
