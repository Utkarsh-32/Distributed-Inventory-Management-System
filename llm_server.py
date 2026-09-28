import json
import time
from concurrent import futures

import grpc
from ollama import chat

import inventory_pb2
import inventory_pb2_grpc


LLM_SERVER_ADDRESS = "localhost:50051"
MODEL = "qwen3:8b"


# ============================================================
# STRUCTURED OUTPUT SCHEMA
# ============================================================

FORECAST_SCHEMA = {
    "type": "object",
    "properties": {
        "forecasts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "item_id": {
                        "type": "string"
                    },
                    "product_name": {
                        "type": "string"
                    },
                    "predicted_7_day_demand": {
                        "type": "integer",
                        "minimum": 0
                    },
                    "trend": {
                        "type": "string"
                    },
                    "reason": {
                        "type": "string"
                    }
                },
                "required": [
                    "item_id",
                    "product_name",
                    "predicted_7_day_demand",
                    "trend",
                    "reason"
                ]
            }
        }
    },
    "required": [
        "forecasts"
    ]
}

REORDER_SCHEMA = {
    "type": "object",
    "properties": {
        "recommendations": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "item_id": {
                        "type": "string"
                    },
                    "product_name": {
                        "type": "string"
                    },
                    "reorder": {
                        "type": "boolean"
                    },
                    "reason": {
                        "type": "string"
                    }
                },
                "required": [
                    "item_id",
                    "product_name",
                    "reorder",
                    "reason"
                ]
            }
        }
    },
    "required": [
        "recommendations"
    ]
}

ANALYTICS_SCHEMA = {
    "type": "object",
    "properties": {
        "observations": {
            "type": "array",
            "items": {
                "type": "string"
            }
        },
        "actions": {
            "type": "array",
            "items": {
                "type": "string"
            }
        }
    },
    "required": [
        "observations",
        "actions"
    ]
}


# ============================================================
# LLM CALL
# ============================================================

def generate_forecast(products):
    """
    Send inventory context to Qwen3 8B through Ollama.

    The model returns structured JSON matching
    FORECAST_SCHEMA.
    """

    system_prompt = """
You are an inventory demand forecasting assistant.

Your task is to estimate total demand for each product
over the next 7 days.

You are given:
- current inventory
- 30 days of historical sales
- recent sales statistics
- trend information

Use the supplied evidence.

Consider:
1. recent demand
2. longer-term demand
3. increasing or decreasing trends
4. unusual variability

Important rules:
- Return JSON only.
- Return exactly one forecast for every supplied product.
- predicted_7_day_demand must be a non-negative integer.
- Do not invent products or historical data.
- Do not calculate reorder quantities.
- A forecast is an estimate, not ground truth.
"""

    user_prompt = (
        "Forecast the next 7 days of demand for all products "
        "in the following inventory context.\n\n"
        + json.dumps(products, indent=2)
    )

    response = chat(
        model=MODEL,

        messages=[
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": user_prompt,
            },
        ],

        format=FORECAST_SCHEMA,

        options={
            "temperature": 0.1,
        },

        think=False,
    )

    content = response.message.content

    return json.loads(content)


def generate_reorder_suggestions(products):
    """
    Ask Qwen3 whether each product should be reordered.

    The application server supplies current stock and the
    already validated 7-day demand forecast.
    """

    system_prompt = """
    You are an automated inventory replenishment assistant.

    For each supplied product, decide whether inventory should be
    reordered based on:

    - current stock
    - predicted demand for the next 7 days

    Decision rule:

    - If current stock is less than predicted 7-day demand:
    reorder = true
    - Otherwise:
    reorder = false

    Important rules:
    - Return JSON only.
    - Return exactly one recommendation for every supplied product.
    - Never invent products.
    - Do not calculate reorder quantities.
    - The application server will calculate the exact quantity.
    - The reason should briefly explain the decision.
    """

    user_prompt = (
        "Decide whether each product should be reordered.\n\n"
        + json.dumps(products, indent=2)
    )

    response = chat(
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": user_prompt,
            },
        ],
        format=REORDER_SCHEMA,
        options={
            "temperature": 0.1,
        },
        think=False,
    )

    return json.loads(response.message.content)


def generate_analytics(data):
    """
    Generate a concise inventory management report
    from facts already validated by the application server.
    """

    system_prompt = """
        You are an inventory analytics and reporting assistant.

        Analyze the validated inventory information supplied by the
        application server.

        Produce:
        1. concise observations about inventory health and demand
        2. practical management actions

        Important rules:
        - Treat the supplied data as ground truth.
        - Do not invent products or facts.
        - Do not contradict the supplied reorder decisions.
        - Do not calculate new values.
        - Focus on useful management interpretation.
        - Return JSON only.
    """

    user_prompt = (
        "Generate an inventory analytics report from the following "
        "validated system data.\n\n"
        + json.dumps(data, indent=2)
    )

    response = chat(
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": user_prompt,
            },
        ],
        format=ANALYTICS_SCHEMA,
        options={
            "temperature": 0.1,
        },
        think=False,
    )

    return json.loads(response.message.content)


# ============================================================
# VALIDATION
# ============================================================

def validate_forecast(result, products):
    """
    Perform basic validation after the LLM responds.

    The LLM is not allowed to silently produce malformed
    application data.
    """

    if not isinstance(result, dict):
        raise ValueError(
            "LLM response is not a JSON object."
        )

    forecasts = result.get("forecasts")

    if not isinstance(forecasts, list):
        raise ValueError(
            "LLM response does not contain forecasts."
        )

    expected_ids = {
        product["item_id"]
        for product in products
    }

    returned_ids = set()

    for forecast in forecasts:

        if not isinstance(forecast, dict):
            raise ValueError(
                "A forecast entry is not an object."
            )

        item_id = forecast.get("item_id")

        if item_id not in expected_ids:
            raise ValueError(
                f"LLM returned unknown item: {item_id}"
            )

        if item_id in returned_ids:
            raise ValueError(
                f"Duplicate forecast for item: {item_id}"
            )

        returned_ids.add(item_id)

        demand = forecast.get(
            "predicted_7_day_demand"
        )

        if not isinstance(demand, int):
            raise ValueError(
                f"Forecast for {item_id} is not an integer."
            )

        if demand < 0:
            raise ValueError(
                f"Forecast for {item_id} is negative."
            )

    missing_ids = expected_ids - returned_ids

    if missing_ids:
        raise ValueError(
            f"LLM did not return forecasts for: "
            f"{sorted(missing_ids)}"
        )

    return result

def validate_reorder(result, products):
    if not isinstance(result, dict):
        raise ValueError(
            "LLM reorder response is not a JSON object."
        )

    recommendations = result.get("recommendations")

    if not isinstance(recommendations, list):
        raise ValueError(
            "LLM response does not contain recommendations."
        )

    expected_ids = {
        product["item_id"]
        for product in products
    }

    returned_ids = set()

    for recommendation in recommendations:

        if not isinstance(recommendation, dict):
            raise ValueError(
                "A reorder recommendation is not an object."
            )

        item_id = recommendation.get("item_id")

        if item_id not in expected_ids:
            raise ValueError(
                f"LLM returned unknown item: {item_id}"
            )

        if item_id in returned_ids:
            raise ValueError(
                f"Duplicate reorder recommendation: {item_id}"
            )

        returned_ids.add(item_id)

        reorder = recommendation.get("reorder")

        if not isinstance(reorder, bool):
            raise ValueError(
                f"Reorder value for {item_id} is not boolean."
            )

    missing_ids = expected_ids - returned_ids

    if missing_ids:
        raise ValueError(
            "LLM did not return reorder decisions for: "
            + ", ".join(sorted(missing_ids))
        )

    return result


def validate_analytics(result):
    if not isinstance(result, dict):
        raise ValueError(
            "LLM analytics response is not a JSON object."
        )

    observations = result.get("observations")
    actions = result.get("actions")

    if not isinstance(observations, list):
        raise ValueError(
            "Analytics response does not contain observations."
        )

    if not isinstance(actions, list):
        raise ValueError(
            "Analytics response does not contain actions."
        )

    for observation in observations:
        if not isinstance(observation, str):
            raise ValueError(
                "Analytics observation is not a string."
            )

    for action in actions:
        if not isinstance(action, str):
            raise ValueError(
                "Analytics action is not a string."
            )

    return result


# ============================================================
# gRPC SERVICE
# ============================================================

class LLMServiceServicer(
    inventory_pb2_grpc.LLMServiceServicer
):

    def getLLMAnswer(self, request, context):

        print()
        print("[LLM SERVER]")
        print(
            f"Request ID: {request.request_id}"
        )
        print(
            f"Task: {request.query}"
        )

        # ----------------------------------------------------
        # Parse context
        # ----------------------------------------------------

        try:

            products = json.loads(
                request.context
            )

        except json.JSONDecodeError:

            return inventory_pb2.LLMAnswerResponse(
                request_id=request.request_id,
                answer=json.dumps(
                    {
                        "error": (
                            "Context is not valid JSON."
                        )
                    }
                ),
            )

        # ----------------------------------------------------
        # Task routing
        # ----------------------------------------------------

        if request.query == "llm_demand_prediction":
            try:
                result = generate_forecast(products)

                result = validate_forecast(
                    result,
                    products,
                )

            except Exception as exc:
                print(f"[LLM ERROR] {exc}")

                return inventory_pb2.LLMAnswerResponse(
                    request_id=request.request_id,
                    answer=json.dumps({
                        "error": str(exc)
                    })
                )

        elif request.query == "llm_reorder_suggestion":
            try:
                result = generate_reorder_suggestions(products)

                result = validate_reorder(
                    result,
                    products,
                )

            except Exception as exc:
                print(f"[LLM ERROR] {exc}")

                return inventory_pb2.LLMAnswerResponse(
                    request_id=request.request_id,
                    answer=json.dumps({
                        "error": str(exc)
                    })
                )

        elif request.query == "llm_analytics":
            if not isinstance(
                products,
                dict
            ):
                raise ValueError(
                    "Analytics context must be a JSON object."
                )

            try:

                result = generate_analytics(
                    products
                )

                result = validate_analytics(
                    result
                )

            except Exception as exc:

                print(
                    f"[LLM ERROR] {exc}"
                )

                return inventory_pb2.LLMAnswerResponse(
                    request_id=request.request_id,
                    answer=json.dumps(
                        {"error": str(exc)}
                    ),
                )

        else:

            return inventory_pb2.LLMAnswerResponse(
                request_id=request.request_id,
                answer=json.dumps({
                    "error": f"Unsupported task: {request.query}"
                })
            )

        # ----------------------------------------------------
        # Return structured JSON
        # ----------------------------------------------------

        answer = json.dumps(
            result,
            indent=2
        )

        print("[LLM SERVER] Forecast completed.")

        return inventory_pb2.LLMAnswerResponse(
            request_id=request.request_id,
            answer=answer,
        )


# ============================================================
# SERVER
# ============================================================

def serve():

    server = grpc.server(
        futures.ThreadPoolExecutor(
            max_workers=4
        )
    )

    inventory_pb2_grpc.add_LLMServiceServicer_to_server(
        LLMServiceServicer(),
        server,
    )

    server.add_insecure_port(
        LLM_SERVER_ADDRESS
    )

    server.start()

    print("========================================")
    print("LLM Server - Node 1")
    print("========================================")
    print(
        f"Listening on: {LLM_SERVER_ADDRESS}"
    )
    print(
        f"Model: {MODEL}"
    )
    print(
        "Runtime: Ollama"
    )
    print("========================================")

    try:

        while True:
            time.sleep(24 * 60 * 60)

    except KeyboardInterrupt:

        print(
            "\nStopping LLM Server..."
        )

        server.stop(0)


if __name__ == "__main__":
    serve()