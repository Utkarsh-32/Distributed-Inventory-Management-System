import json

from ollama import chat


MODEL = "qwen3:8b"


def main():
    schema = {
        "type": "object",
        "properties": {
            "predicted_7_day_demand": {
                "type": "integer",
                "minimum": 0,
            },
            "trend": {
                "type": "string",
            },
            "reason": {
                "type": "string",
            },
        },
        "required": [
            "predicted_7_day_demand",
            "trend",
            "reason",
        ],
    }

    sales = [
        4, 5, 4, 6, 5, 7, 5, 6, 7, 8,
        7, 8, 9, 8, 9, 10, 8, 9, 10, 11,
        9, 10, 11, 10, 12, 11, 12, 13, 12, 14,
    ]

    response = chat(
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an inventory demand forecasting "
                    "assistant. Analyze the supplied historical "
                    "sales and return only the requested JSON."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Forecast total demand for the next 7 days.\n\n"
                    f"30-day daily sales: {sales}"
                ),
            },
        ],
        format=schema,
        options={
            "temperature": 0.2,
        },
        think=False,
    )

    print("Raw model content:")
    print(response.message.content)

    result = json.loads(response.message.content)

    print("\nParsed result:")
    print("Predicted demand:", result["predicted_7_day_demand"])
    print("Trend:", result["trend"])
    print("Reason:", result["reason"])


if __name__ == "__main__":
    main()