import json
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import app_server


class FakeLLMStub:
    def __init__(self):
        self.calls = []

    def getLLMAnswer(self, request, timeout):
        self.calls.append((request.query, timeout))

        if request.query == "llm_demand_prediction":
            products = json.loads(request.context)
            return SimpleNamespace(
                answer=json.dumps(
                    {
                        "forecasts": [
                            {
                                "item_id": product["item_id"],
                                "product_name": product["product_name"],
                                "predicted_7_day_demand": 10,
                                "trend": "stable",
                                "reason": "Recent demand is stable.",
                            }
                            for product in products
                        ]
                    }
                )
            )

        if request.query == "llm_analytics":
            return SimpleNamespace(
                answer=json.dumps(
                    {
                        "observations": ["Demand is stable."],
                    }
                )
            )

        raise AssertionError(f"Unexpected LLM request: {request.query}")


class DashboardOptimizationTests(unittest.TestCase):
    def setUp(self):
        self.service = object.__new__(app_server.ClientServiceServicer)
        self.service.llm_stub = FakeLLMStub()
        self.service.dashboard_lock = threading.Lock()
        self.service.dashboard_cache = None
        self.service.dashboard_cache_created_at = 0.0

        history = [4] * 30
        self.products = [
            {
                "item_id": "item_101",
                "product_name": "Laptop",
                "current_stock": 50,
                "sales_history": history,
            },
            {
                "item_id": "item_102",
                "product_name": "Mouse",
                "current_stock": 5,
                "sales_history": history,
            },
        ]

    @patch("app_server.database.get_products_with_history")
    def test_dashboard_uses_one_model_request_and_caches_result(self, products):
        products.return_value = self.products

        first = self.service._get_cached_llm_dashboard()
        second = self.service._get_cached_llm_dashboard()

        self.assertIs(first, second)
        self.assertEqual(
            self.service.llm_stub.calls,
            [
                ("llm_analytics", app_server.LLM_REQUEST_TIMEOUT_SECONDS),
            ],
        )
        self.assertEqual(len(first["forecasts"]), 2)
        self.assertEqual(
            first["reorder_recommendations"][1]["reorder"],
            True,
        )

    def test_invalidate_dashboard_cache(self):
        self.service.dashboard_cache = {"cached": True}
        self.service.dashboard_cache_created_at = 1.0

        self.service._invalidate_dashboard_cache()

        self.assertIsNone(self.service.dashboard_cache)
        self.assertEqual(self.service.dashboard_cache_created_at, 0.0)

    @patch("app_server.database.get_products_with_history")
    def test_dashboard_keeps_calculated_analytics_when_insights_fail(self, products):
        products.return_value = self.products
        self.service.llm_stub = SimpleNamespace(
            getLLMAnswer=lambda request, timeout: SimpleNamespace(
                answer=json.dumps({"error": "Ollama is unavailable"})
            )
        )

        dashboard = self.service._get_llm_dashboard()

        self.assertEqual(len(dashboard["forecasts"]), 2)
        self.assertEqual(dashboard["analytics"]["llm_insights"], [])
        self.assertEqual(
            dashboard["analytics"]["llm_error"],
            "Ollama is unavailable",
        )


if __name__ == "__main__":
    unittest.main()
