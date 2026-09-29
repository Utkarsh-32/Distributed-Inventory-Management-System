import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import database


class OrderForecastHistoryTests(unittest.TestCase):
    def test_successful_order_updates_the_latest_sales_observation(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "inventory.db"

            with patch.object(database, "DB_PATH", db_path):
                database.initialize_database()
                conn = database.get_connection()
                try:
                    conn.execute(
                        """
                        INSERT INTO users
                            (id, username, password_hash, created_at)
                        VALUES
                            (1, 'tester', 'unused', 'now')
                        """
                    )
                    conn.execute(
                        """
                        INSERT INTO products
                            (item_id, name, stock)
                        VALUES
                            ('item_104', 'Monitor', 30)
                        """
                    )
                    conn.executemany(
                        """
                        INSERT INTO sales_history
                            (item_id, day_index, units_sold)
                        VALUES
                            ('item_104', ?, ?)
                        """,
                        [(day, 0) for day in range(1, 31)],
                    )
                    conn.commit()
                finally:
                    conn.close()

                first_order = database.place_order(1, "item_104", 10)
                second_order = database.place_order(1, "item_104", 2)
                product = database.get_products_with_history()[0]

        self.assertTrue(first_order["success"])
        self.assertTrue(second_order["success"])
        self.assertEqual(product["current_stock"], 18)
        self.assertEqual(product["sales_history"][-7:], [0, 0, 0, 0, 0, 0, 12])
        self.assertEqual(sum(product["sales_history"][-7:]) / 7, 12 / 7)

    def test_existing_successful_orders_are_recorded_once(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "inventory.db"

            with patch.object(database, "DB_PATH", db_path):
                database.initialize_database()
                conn = database.get_connection()
                try:
                    conn.execute(
                        """
                        INSERT INTO users
                            (id, username, password_hash, created_at)
                        VALUES
                            (1, 'tester', 'unused', 'now')
                        """
                    )
                    conn.execute(
                        """
                        INSERT INTO products
                            (item_id, name, stock)
                        VALUES
                            ('item_104', 'Monitor', 20)
                        """
                    )
                    conn.executemany(
                        """
                        INSERT INTO sales_history
                            (item_id, day_index, units_sold)
                        VALUES
                            ('item_104', ?, ?)
                        """,
                        [(day, 0) for day in range(1, 31)],
                    )
                    conn.execute(
                        """
                        INSERT INTO orders
                            (user_id, item_id, quantity, status, created_at)
                        VALUES
                            (1, 'item_104', 10, 'SUCCESS', 'now')
                        """
                    )
                    conn.commit()
                finally:
                    conn.close()

                database.initialize_database()
                database.initialize_database()
                product = database.get_products_with_history()[0]

                conn = database.get_connection()
                try:
                    recorded = conn.execute(
                        """
                        SELECT sales_history_recorded
                        FROM orders
                        WHERE item_id = 'item_104'
                        """
                    ).fetchone()
                finally:
                    conn.close()

        self.assertEqual(product["sales_history"][-1], 10)
        self.assertEqual(recorded["sales_history_recorded"], 1)


if __name__ == "__main__":
    unittest.main()
