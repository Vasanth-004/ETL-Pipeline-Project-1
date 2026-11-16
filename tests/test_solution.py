import contextlib
import io
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path
from contextlib import closing

import pandas as pd

import solution


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data_dir = solution.DATASET_DIR
        cls.customers = pd.read_csv(data_dir / "customers.csv")
        cls.products = pd.read_csv(data_dir / "products.csv")
        cls.orders = pd.read_csv(data_dir / "orders.csv")
        cls.new_orders = pd.read_csv(data_dir / "new_orders.csv")

    def test_orders_are_transformed_and_sales_are_correct(self):
        valid, rejected = solution.validate_and_transform_orders(
            self.orders, self.customers, self.products
        )

        self.assertEqual(len(valid), 6)
        self.assertTrue(rejected.empty)
        self.assertEqual(valid["order_amount"].sum(), 160400)
        self.assertEqual(
            valid.groupby("customer_name")["order_amount"].sum().to_dict(),
            {"Anil": 2400, "Priya": 3000, "Ravi": 70000, "Suresh": 85000},
        )

    def test_invalid_orders_are_rejected(self):
        orders = self.orders.iloc[:2].copy()
        orders.loc[orders.index[0], "quantity"] = 0
        orders.loc[orders.index[1], "customer_id"] = 9999

        valid, rejected = solution.validate_and_transform_orders(
            orders, self.customers, self.products
        )

        self.assertTrue(valid.empty)
        self.assertEqual(len(rejected), 2)

    def test_elt_skips_invalid_orders(self):
        orders = self.orders.copy()
        orders.loc[orders.index[0], "quantity"] = 0

        valid, _ = solution.validate_and_transform_orders(
            orders, self.customers, self.products
        )
        with closing(sqlite3.connect(":memory:")) as database:
            solution.create_tables(database)
            raw_count, transformed_count = solution.run_elt(
                database, orders, self.customers, self.products
            )

        self.assertEqual(raw_count, len(orders))
        self.assertEqual(transformed_count, len(valid))

    def test_incremental_load_does_not_duplicate_orders(self):
        with closing(sqlite3.connect(":memory:")) as database:
            solution.create_tables(database)

            self.assertEqual(solution.full_load(database, self.orders), 6)
            self.assertEqual(solution.incremental_load(database, self.orders), 6)
            self.assertEqual(solution.incremental_load(database, self.new_orders), 2)
            self.assertEqual(solution.incremental_load(database, self.new_orders), 0)
            self.assertEqual(
                database.execute(
                    "SELECT COUNT(*) FROM incremental_orders"
                ).fetchone()[0],
                8,
            )

    def test_full_load_replaces_the_target(self):
        with closing(sqlite3.connect(":memory:")) as database:
            solution.create_tables(database)

            solution.full_load(database, self.orders)
            self.assertEqual(solution.full_load(database, self.new_orders), 2)
            self.assertEqual(
                database.execute(
                    "SELECT MIN(order_id), MAX(order_id) FROM full_load_orders"
                ).fetchone(),
                (607, 608),
            )

    def test_invalid_new_orders_are_rejected_not_loaded(self):
        with tempfile.TemporaryDirectory() as folder:
            project_dir = Path(folder)
            data_dir = project_dir / "datasets"
            data_dir.mkdir()
            for name in ["customers.csv", "products.csv", "orders.csv", "new_orders.csv"]:
                shutil.copy(solution.DATASET_DIR / name, data_dir / name)

            new_orders = pd.read_csv(data_dir / "new_orders.csv")
            new_orders.loc[0, "quantity"] = 0
            new_orders.to_csv(data_dir / "new_orders.csv", index=False)
            output = io.StringIO()

            with contextlib.redirect_stdout(output):
                solution.run_pipeline(project_dir=project_dir)

            self.assertIn("Invalid records: 1", output.getvalue())
            self.assertIn("Pipeline status: SUCCESS", output.getvalue())
            with closing(sqlite3.connect(project_dir / "output" / "ecommerce.db")) as database:
                self.assertEqual(
                    database.execute(
                        "SELECT COUNT(*) FROM incremental_orders"
                    ).fetchone()[0],
                    7,
                )
                self.assertEqual(
                    database.execute(
                        "SELECT COUNT(*) FROM rejected_orders"
                    ).fetchone()[0],
                    1,
                )

    def test_pipeline_runs_and_saves_expected_results(self):
        with tempfile.TemporaryDirectory() as folder:
            database_path = Path(folder) / "ecommerce.db"
            output = io.StringIO()
            second_output = io.StringIO()

            with contextlib.redirect_stdout(output):
                solution.run_pipeline(db_path=database_path)
            with contextlib.redirect_stdout(second_output):
                solution.run_pipeline(db_path=database_path)

            expected_output = [
                "SOURCE: E-commerce CSV files",
                "EXTRACT: Read customer, product and order records",
                "TRANSFORM: Validate and calculate order amounts",
                "LOAD: Store processed records in a database",
                "MONITOR: Display counts and pipeline status",
                "Rows: 6",
                "Columns: 6",
                "First order IDs: 601, 602, 603, 604, 605",
                "Column names: order_id, customer_id, product_id, quantity, order_date, ingestion_date",
                "Valid orders: 6",
                "Invalid orders: 0",
                "Total sales: 160400",
                "Clean records loaded: 6",
                "Raw records loaded: 6",
                "Transformed records: 6",
                "Bronze rows: 6",
                "Silver rows: 6",
                "Gold customer rows: 4",
                "Total Gold sales: 160400",
                "Records in full-load target: 6",
                "Existing target records: 6",
                "New source records: 2",
                "New records inserted: 2",
                "Final target records: 8",
                "Second execution:",
                "Inserted: 0",
                "Target count: 8",
                "Source records processed: 8",
                "Valid records: 8",
                "Rejected records: 0",
                "Pipeline status: SUCCESS",
            ]
            for expected in expected_output:
                with self.subTest(output=expected):
                    self.assertIn(expected, output.getvalue())
            self.assertIn("Existing target records: 8", second_output.getvalue())
            self.assertIn("New records inserted: 0", second_output.getvalue())
            self.assertIn("Pipeline status: SUCCESS", second_output.getvalue())

            with closing(sqlite3.connect(database_path)) as database:
                expected_counts = {
                    "bronze_orders": 6,
                    "silver_order_details": 6,
                    "gold_customer_sales": 4,
                    "etl_order_details": 6,
                    "elt_raw_orders": 6,
                    "elt_order_details": 6,
                    "full_load_orders": 6,
                    "incremental_orders": 8,
                    "rejected_orders": 0,
                }
                for table, expected_count in expected_counts.items():
                    with self.subTest(table=table):
                        count = database.execute(
                            f"SELECT COUNT(*) FROM {table}"
                        ).fetchone()[0]
                        self.assertEqual(count, expected_count)
                self.assertEqual(
                    database.execute(
                        "SELECT SUM(total_sales) FROM gold_customer_sales"
                    ).fetchone()[0],
                    160400,
                )
                summary = dict(
                    database.execute(
                        "SELECT metric, value FROM pipeline_run_summary"
                    )
                )
                self.assertEqual(summary["idempotency"], "PASSED")
                self.assertEqual(summary["pipeline_status"], "SUCCESS")
                self.assertEqual(
                    database.execute("PRAGMA integrity_check").fetchone()[0],
                    "ok",
                )


if __name__ == "__main__":
    unittest.main()
