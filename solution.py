"""Simple e-commerce pipeline example using Pandas and SQLite."""

import math
import sqlite3
from pathlib import Path

import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parent
DATASET_DIR = PROJECT_DIR / "datasets"

ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "order_date",
    "ingestion_date",
]
DETAIL_COLUMNS = ORDER_COLUMNS + [
    "customer_name",
    "product_name",
    "category",
    "unit_price",
    "order_amount",
]


def read_csv(path, required_columns):
    """Read a CSV file and check that it has the needed columns."""
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")

    data = pd.read_csv(path)
    missing_columns = [name for name in required_columns if name not in data.columns]
    if missing_columns:
        raise ValueError(f"{path.name} is missing: {', '.join(missing_columns)}")
    return data


def positive_integer(value):
    """Return a positive integer, or None when the value is invalid."""
    if pd.isna(value):
        return None

    try:
        number = float(value)
    except (TypeError, ValueError):
        return None

    if number <= 0 or not number.is_integer():
        return None
    return int(number)


def validate_and_transform_orders(orders, customers, products):
    """Return enriched valid orders and invalid orders with reasons."""
    customer_names = {
        positive_integer(row.customer_id): row.customer_name
        for row in customers.itertuples(index=False)
        if positive_integer(row.customer_id) is not None
    }
    product_details = {
        positive_integer(row.product_id): row
        for row in products.itertuples(index=False)
        if positive_integer(row.product_id) is not None
    }

    valid_orders = []
    rejected_orders = []
    seen_order_ids = set()

    for order in orders.to_dict(orient="records"):
        row = {name: order.get(name) for name in ORDER_COLUMNS}
        reasons = []
        numbers = {}

        for name in ["order_id", "customer_id", "product_id", "quantity"]:
            number = positive_integer(row[name])
            if number is None:
                reasons.append(f"{name} must be a positive integer")
            else:
                numbers[name] = number

        for name in ["order_date", "ingestion_date"]:
            if pd.isna(row[name]) or not str(row[name]).strip():
                reasons.append(f"{name} is required")

        order_id = numbers.get("order_id")
        if order_id in seen_order_ids:
            reasons.append("duplicate order_id")
        if order_id is not None:
            seen_order_ids.add(order_id)

        customer_id = numbers.get("customer_id")
        product_id = numbers.get("product_id")
        if customer_id not in customer_names:
            reasons.append("customer_id does not exist")
        if product_id not in product_details:
            reasons.append("product_id does not exist")

        product = product_details.get(product_id)
        price = None
        if product is not None:
            try:
                price = float(product.unit_price)
            except (TypeError, ValueError):
                reasons.append("unit_price must be a number")
            if price is not None and (not math.isfinite(price) or price < 0):
                reasons.append("unit_price must be zero or more")

        if reasons:
            rejected_orders.append(
                {**row, "rejection_reason": "; ".join(reasons)}
            )
            continue

        valid_orders.append(
            {
                **row,
                **numbers,
                "customer_name": customer_names[customer_id],
                "product_name": product.product_name,
                "category": product.category,
                "unit_price": price,
                "order_amount": numbers["quantity"] * price,
            }
        )

    valid = pd.DataFrame(valid_orders, columns=DETAIL_COLUMNS)
    rejected = pd.DataFrame(
        rejected_orders, columns=ORDER_COLUMNS + ["rejection_reason"]
    )
    return valid, rejected


def create_tables(connection):
    """Create the tables used in the examples."""
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS bronze_orders (
            order_id, customer_id, product_id, quantity, order_date, ingestion_date
        );
        CREATE TABLE IF NOT EXISTS silver_order_details (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER, product_id INTEGER, quantity INTEGER,
            order_date TEXT, ingestion_date TEXT, customer_name TEXT,
            product_name TEXT, category TEXT, unit_price REAL, order_amount REAL
        );
        CREATE TABLE IF NOT EXISTS gold_customer_sales (
            customer_id INTEGER PRIMARY KEY, customer_name TEXT,
            total_orders INTEGER, total_sales REAL
        );
        CREATE TABLE IF NOT EXISTS etl_order_details (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER, product_id INTEGER, quantity INTEGER,
            order_date TEXT, ingestion_date TEXT, customer_name TEXT,
            product_name TEXT, category TEXT, unit_price REAL, order_amount REAL
        );
        CREATE TABLE IF NOT EXISTS elt_raw_orders (
            order_id, customer_id, product_id, quantity, order_date, ingestion_date
        );
        CREATE TABLE IF NOT EXISTS elt_customers (
            customer_id INTEGER PRIMARY KEY, customer_name TEXT
        );
        CREATE TABLE IF NOT EXISTS elt_products (
            product_id INTEGER PRIMARY KEY, product_name TEXT,
            category TEXT, unit_price REAL
        );
        CREATE TABLE IF NOT EXISTS elt_order_details (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER, product_id INTEGER, quantity INTEGER,
            order_date TEXT, ingestion_date TEXT, customer_name TEXT,
            product_name TEXT, category TEXT, unit_price REAL, order_amount REAL
        );
        CREATE TABLE IF NOT EXISTS full_load_orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER, product_id INTEGER, quantity INTEGER,
            order_date TEXT, ingestion_date TEXT
        );
        CREATE TABLE IF NOT EXISTS incremental_orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER, product_id INTEGER, quantity INTEGER,
            order_date TEXT, ingestion_date TEXT
        );
        CREATE TABLE IF NOT EXISTS rejected_orders (
            order_id, customer_id, product_id, quantity, order_date,
            ingestion_date, rejection_reason TEXT
        );
        CREATE TABLE IF NOT EXISTS pipeline_run_summary (
            metric TEXT PRIMARY KEY, value TEXT
        );
        """
    )


def reset_tables(connection):
    """Clear refreshed outputs but keep the incremental target between runs."""
    tables = [
        "bronze_orders",
        "silver_order_details",
        "gold_customer_sales",
        "etl_order_details",
        "elt_raw_orders",
        "elt_customers",
        "elt_products",
        "elt_order_details",
        "full_load_orders",
        "rejected_orders",
        "pipeline_run_summary",
    ]
    for table in tables:
        connection.execute(f"DELETE FROM {table}")


def insert_rows(connection, table, data, columns):
    """Insert DataFrame rows into a table and return the inserted row count."""
    rows = list(data[columns].itertuples(index=False, name=None))
    if not rows:
        return 0

    marks = ", ".join(["?"] * len(columns))
    names = ", ".join(columns)
    connection.executemany(
        f"INSERT INTO {table} ({names}) VALUES ({marks})",
        rows,
    )
    return len(rows)


def run_etl(connection, orders, customers, products):
    """Clean the data in Python, then load the results."""
    valid, _ = validate_and_transform_orders(orders, customers, products)
    connection.execute("DELETE FROM etl_order_details")
    return insert_rows(connection, "etl_order_details", valid, DETAIL_COLUMNS)


def run_elt(connection, orders, customers, products):
    """Load raw rows first, then transform them with a SQL query."""
    for table in ["elt_raw_orders", "elt_customers", "elt_products", "elt_order_details"]:
        connection.execute(f"DELETE FROM {table}")

    raw_count = insert_rows(connection, "elt_raw_orders", orders, ORDER_COLUMNS)
    insert_rows(
        connection, "elt_customers", customers, ["customer_id", "customer_name"]
    )
    insert_rows(
        connection,
        "elt_products",
        products,
        ["product_id", "product_name", "category", "unit_price"],
    )

    connection.execute(
        """
        INSERT INTO elt_order_details (
            order_id, customer_id, product_id, quantity, order_date,
            ingestion_date, customer_name, product_name, category,
            unit_price, order_amount
        )
        SELECT
            orders.order_id, orders.customer_id, orders.product_id,
            orders.quantity, orders.order_date, orders.ingestion_date,
            customers.customer_name, products.product_name, products.category,
            products.unit_price, orders.quantity * products.unit_price
        FROM elt_raw_orders AS orders
        JOIN elt_customers AS customers
            ON customers.customer_id = orders.customer_id
        JOIN elt_products AS products
            ON products.product_id = orders.product_id
        WHERE CAST(orders.order_id AS INTEGER) > 0
          AND CAST(orders.order_id AS REAL) = CAST(orders.order_id AS INTEGER)
          AND CAST(orders.customer_id AS INTEGER) > 0
          AND CAST(orders.customer_id AS REAL) = CAST(orders.customer_id AS INTEGER)
          AND CAST(orders.product_id AS INTEGER) > 0
          AND CAST(orders.product_id AS REAL) = CAST(orders.product_id AS INTEGER)
          AND CAST(orders.quantity AS INTEGER) > 0
          AND CAST(orders.quantity AS REAL) = CAST(orders.quantity AS INTEGER)
          AND TRIM(COALESCE(orders.order_date, '')) <> ''
          AND TRIM(COALESCE(orders.ingestion_date, '')) <> ''
          AND typeof(products.unit_price) IN ('integer', 'real')
          AND products.unit_price >= 0
          AND products.unit_price <= 1.7976931348623157e308
          AND NOT EXISTS (
              SELECT 1
              FROM elt_raw_orders AS previous
              WHERE previous.order_id = orders.order_id
                AND previous.rowid < orders.rowid
          )
        """
    )
    transformed_count = connection.execute(
        "SELECT COUNT(*) FROM elt_order_details"
    ).fetchone()[0]
    return raw_count, transformed_count


def build_medallion_layers(connection, orders, valid, rejected):
    """Load raw Bronze, clean Silver, and customer-total Gold data."""
    for table in [
        "bronze_orders",
        "silver_order_details",
        "gold_customer_sales",
        "rejected_orders",
    ]:
        connection.execute(f"DELETE FROM {table}")

    bronze_count = insert_rows(connection, "bronze_orders", orders, ORDER_COLUMNS)
    silver_count = insert_rows(
        connection, "silver_order_details", valid, DETAIL_COLUMNS
    )
    insert_rows(
        connection,
        "rejected_orders",
        rejected,
        ORDER_COLUMNS + ["rejection_reason"],
    )

    gold = (
        valid.groupby(["customer_id", "customer_name"], as_index=False)
        .agg(total_orders=("order_id", "count"), total_sales=("order_amount", "sum"))
        .sort_values("customer_id")
    )
    gold_count = insert_rows(
        connection,
        "gold_customer_sales",
        gold,
        ["customer_id", "customer_name", "total_orders", "total_sales"],
    )
    total_sales = gold["total_sales"].sum() if not gold.empty else 0
    return bronze_count, silver_count, gold_count, total_sales


def full_load(connection, orders):
    """Replace the full-load table with all source orders."""
    connection.execute("DELETE FROM full_load_orders")
    insert_rows(connection, "full_load_orders", orders, ORDER_COLUMNS)
    return connection.execute("SELECT COUNT(*) FROM full_load_orders").fetchone()[0]


def incremental_load(connection, orders):
    """Insert only new order IDs; repeated IDs are ignored."""
    rows = list(orders[ORDER_COLUMNS].itertuples(index=False, name=None))
    before = connection.total_changes
    connection.executemany(
        """
        INSERT INTO incremental_orders
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(order_id) DO NOTHING
        """,
        rows,
    )
    return connection.total_changes - before


def run_pipeline(project_dir=PROJECT_DIR, db_path=None):
    """Run all nine tasks and save the results in SQLite."""
    if db_path is None:
        db_path = project_dir / "output" / "ecommerce.db"

    data_dir = project_dir / "datasets"
    # The CSV files are the source; this Python/Pandas script extracts their data.
    customers = read_csv(
        data_dir / "customers.csv",
        ["customer_id", "customer_name", "city", "updated_at"],
    )
    products = read_csv(
        data_dir / "products.csv",
        ["product_id", "product_name", "category", "unit_price"],
    )
    orders = read_csv(data_dir / "orders.csv", ORDER_COLUMNS)
    new_orders = read_csv(data_dir / "new_orders.csv", ORDER_COLUMNS)
    valid_new_orders, rejected_new_orders = validate_and_transform_orders(
        new_orders, customers, products
    )

    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)

    try:
        create_tables(connection)
        reset_tables(connection)

        print("SOURCE: E-commerce CSV files")
        print("EXTRACT: Read customer, product and order records")
        print("TRANSFORM: Validate and calculate order amounts")
        print("LOAD: Store processed records in a database")
        print("MONITOR: Display counts and pipeline status")

        print("\nTASK 2 - EXTRACT AND INSPECT")
        print(f"Rows: {len(orders)}")
        print(f"Columns: {len(orders.columns)}")
        print("First five records:")
        print(orders.head().to_string(index=False))
        first_ids = ", ".join(str(value) for value in orders["order_id"].head())
        print(f"First order IDs: {first_ids}")
        print(f"Column names: {', '.join(orders.columns)}")

        valid, rejected = validate_and_transform_orders(orders, customers, products)
        print("\nTASK 3 - VALIDATE AND TRANSFORM")
        print(f"Valid orders: {len(valid)}")
        print(f"Invalid orders: {len(rejected)}")
        print("Order amounts:")
        for row in valid.to_dict(orient="records"):
            print(f"{row['order_id']} = {row['order_amount']:g}")
        total_sales = valid["order_amount"].sum()
        print(f"Total sales: {total_sales:g}")

        print("\nTASK 4 - ETL AND ELT")
        etl_count = run_etl(connection, orders, customers, products)
        print("ETL:")
        print("Transformation performed before loading.")
        print(f"Clean records loaded: {etl_count}")
        raw_count, elt_count = run_elt(connection, orders, customers, products)
        print("\nELT:")
        print(f"Raw records loaded: {raw_count}")
        print("Transformation performed inside database.")
        print(f"Transformed records: {elt_count}")
        if elt_count != len(valid):
            raise RuntimeError("ELT output does not match the validated order count.")

        print("\nTASK 5 - BRONZE, SILVER AND GOLD")
        bronze_count, silver_count, gold_count, gold_sales = build_medallion_layers(
            connection, orders, valid, rejected
        )
        print(f"Bronze rows: {bronze_count}")
        print(f"Silver rows: {silver_count}")
        print(f"Gold customer rows: {gold_count}")
        print(f"Total Gold sales: {gold_sales:g}")

        print("\nTASK 6 - FULL LOAD")
        print("Full load started")
        print(f"Source records: {len(orders)}")
        full_count = full_load(connection, orders)
        full_status = "SUCCESS" if full_count == len(orders) else "FAILED"
        print(f"Records in full-load target: {full_count}")
        print(f"Full load status: {full_status}")

        print("\nTASK 7 - INCREMENTAL LOAD")
        incremental_load(connection, valid[ORDER_COLUMNS])
        existing_count = connection.execute(
            "SELECT COUNT(*) FROM incremental_orders"
        ).fetchone()[0]
        print(f"Existing target records: {existing_count}")
        print(f"New source records: {len(new_orders)}")
        inserted_count = incremental_load(
            connection, valid_new_orders[ORDER_COLUMNS]
        )
        insert_rows(
            connection,
            "rejected_orders",
            rejected_new_orders,
            ORDER_COLUMNS + ["rejection_reason"],
        )
        target_count = connection.execute(
            "SELECT COUNT(*) FROM incremental_orders"
        ).fetchone()[0]
        print(f"New records inserted: {inserted_count}")
        print(f"Final target records: {target_count}")
        expected_ids = set(valid["order_id"]) | set(valid_new_orders["order_id"])
        loaded_ids = {
            row[0]
            for row in connection.execute("SELECT order_id FROM incremental_orders")
        }
        incremental_status = (
            "SUCCESS" if expected_ids.issubset(loaded_ids) else "FAILED"
        )
        print(f"Incremental load status: {incremental_status}")

        print("\nTASK 8 - IDEMPOTENT LOAD")
        print("First execution:")
        print(f"Inserted: {inserted_count}")
        print(f"Target count: {target_count}")
        repeated_insert_count = incremental_load(
            connection, valid_new_orders[ORDER_COLUMNS]
        )
        repeated_target_count = connection.execute(
            "SELECT COUNT(*) FROM incremental_orders"
        ).fetchone()[0]
        idempotent = (
            repeated_insert_count == 0 and repeated_target_count == target_count
        )
        print("\nSecond execution:")
        print(f"Inserted: {repeated_insert_count}")
        print(f"Target count: {repeated_target_count}")
        print(f"Idempotency: {'PASSED' if idempotent else 'FAILED'}")

        pipeline_ok = (
            etl_count == len(valid)
            and raw_count == len(orders)
            and elt_count == len(valid)
            and bronze_count == len(orders)
            and silver_count == len(valid)
            and gold_sales == total_sales
            and gold_count == valid["customer_id"].nunique()
            and full_status == "SUCCESS"
            and incremental_status == "SUCCESS"
            and inserted_count <= len(valid_new_orders)
            and idempotent
        )
        pipeline_status = "SUCCESS" if pipeline_ok else "FAILED"
        summary = {
            "extracted": len(orders) + len(new_orders),
            "valid": len(valid) + len(valid_new_orders),
            "invalid": len(rejected) + len(rejected_new_orders),
            "inserted": inserted_count,
            "rejected": len(rejected) + len(rejected_new_orders),
            "full_load_status": full_status,
            "incremental_load_status": incremental_status,
            "idempotency": "PASSED" if idempotent else "FAILED",
            "pipeline_status": pipeline_status,
        }
        connection.executemany(
            "INSERT INTO pipeline_run_summary VALUES (?, ?)",
            [(name, str(value)) for name, value in summary.items()],
        )

        print("\nTASK 9 - PIPELINE SUMMARY")
        print("WEEK 10 DAY 46 PIPELINE SUMMARY")
        print(f"Source records processed: {summary['extracted']}")
        print(f"Valid records: {summary['valid']}")
        print(f"Invalid records: {summary['invalid']}")
        print(f"New records inserted: {summary['inserted']}")
        print(f"Rejected records: {summary['rejected']}")
        print(f"Duplicate-safe rerun: {summary['idempotency']}")
        print(f"Full load: {summary['full_load_status']}")
        print(f"Incremental load: {summary['incremental_load_status']}")
        print(f"Idempotency: {summary['idempotency']}")
        print(f"Pipeline status: {pipeline_status}")

        if not pipeline_ok:
            raise RuntimeError("Pipeline checks failed; see the run summary.")
        connection.commit()
        print(f"\nSQLite database: {db_path}")
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    run_pipeline()
