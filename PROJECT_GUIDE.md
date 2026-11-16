# Project 1: E-Commerce ETL Pipeline

## Code and folder guide

This guide explains the files, tools, data flow, and code in this Day 46
project. It is based on the files currently in the project folder.

## What this project does

The project reads sample customer, product, and order CSV files; checks order
data; calculates sales; and saves results into a local SQLite database. It
demonstrates all nine Day 46 workbook tasks:

1. Print the pipeline stages.
2. Read and inspect the order data.
3. Validate orders and calculate order amounts.
4. Compare ETL and ELT.
5. Build Bronze, Silver, and Gold data layers.
6. Replace a full-load target.
7. Insert new orders incrementally.
8. Prove that rerunning an incremental batch does not duplicate rows.
9. Print and save a pipeline run summary.

## Folder map

| Path | What it is and why it is here |
|---|---|
| `solution.py` | Main runnable program. It contains the pipeline logic and creates the SQLite tables. |
| `tests/test_solution.py` | Automated checks for calculations, invalid data, loading behavior, and end-to-end runs. |
| `datasets/customers.csv` | Customer IDs and names used to enrich orders and aggregate customer sales. |
| `datasets/products.csv` | Product names, categories, and unit prices used to calculate sales. |
| `datasets/orders.csv` | Six historical orders used for extraction, transformation, and full loading. |
| `datasets/new_orders.csv` | Two additional orders used for incremental loading and duplicate-safe reruns. |
| `output/ecommerce.db` | Generated SQLite database containing the output tables. It is an output, not source code. |
| `requirements.txt` | Lists Pandas, the only non-standard Python package used by the pipeline and tests. |
| `.gitignore` | Prevents virtual environments, Python bytecode, and OS files from cluttering Git. |
| `README.md` | Short setup and run instructions for someone cloning the project. |
| `D100_10.11.2026_week10 _D1(Day46) Project-1.md` | Original workbook specification used to define project requirements. |

## Tools and why they are used

- **Python** runs the pipeline and test suite.
- **Pandas** reads CSV files and makes it straightforward to inspect and
  aggregate tabular data.
- **SQLite (`sqlite3`)** is included with Python and provides a local SQL
  database without a separate server.
- **`pathlib`** builds paths relative to the project, so the script can be run
  from another current working directory.
- **`math`** checks that product prices are finite numbers before calculations.
- **`unittest`** checks expected behavior using Python's standard library.

## `solution.py`: code walkthrough

### Imports, paths, and column lists

The imports provide CSV/table processing (`pandas`), local SQL access
(`sqlite3`), project-relative file paths (`Path`), and safe numeric validation
(`math`). `PROJECT_DIR` is based on the location of `solution.py`, and
`DATASET_DIR` points to the `datasets` folder. This avoids depending on where a
terminal happens to be opened.

`ORDER_COLUMNS` lists the six raw order fields expected in both order files.
`DETAIL_COLUMNS` adds customer/product descriptions, unit price, and calculated
order amount for the enriched Silver and ETL/ELT tables.

### Input reading and validation helpers

- **`read_csv(path, required_columns)`** checks that a file exists, reads it
  with Pandas, and reports missing columns with a useful error.
- **`positive_integer(value)`** converts a positive whole-number value to an
  integer. Missing, non-numeric, fractional, zero, or negative identifiers and
  quantities are treated as invalid.
- **`validate_and_transform_orders(orders, customers, products)`** builds
  customer and product lookups, checks each order, records a reason for rejected
  rows, joins valid orders to their customer/product information, and computes
  `quantity * unit_price`. It returns two DataFrames: valid enriched orders and
  rejected orders.

### Database setup and common insert helper

- **`create_tables(connection)`** creates the output tables if they do not
  already exist. `order_id` is a primary key in the full-load, incremental,
  Silver, ETL, and ELT result tables where uniqueness is expected.
- **`reset_tables(connection)`** clears the demonstration and reporting tables
  before rebuilding them. It intentionally does not clear
  `incremental_orders`; preserving that table lets a later run prove persistent
  idempotency.
- **`insert_rows(connection, table, data, columns)`** inserts the selected
  DataFrame columns into a named table and reports how many rows were inserted.

### ETL, ELT, and Medallion layers

- **`run_etl(...)`** validates and enriches orders in Python first, then inserts
  the clean result into `etl_order_details`.
- **`run_elt(...)`** first writes raw orders, customers, and products into
  staging tables. A SQLite `INSERT ... SELECT` joins and calculates the
  transformed orders inside the database. SQL filters invalid keys, quantities,
  dates, and prices, and excludes duplicate order IDs in the raw batch.
- **`build_medallion_layers(...)`** writes the original order fields to Bronze,
  valid enriched rows to Silver, rejected rows with reasons to
  `rejected_orders`, and customer order counts/sales to Gold. Bronze is kept
  separate from cleaning so it preserves the source values.

### Full and incremental loading

- **`full_load(connection, orders)`** clears `full_load_orders`, inserts the
  entire supplied DataFrame, and returns the resulting row count.
- **`incremental_load(connection, orders)`** inserts rows into
  `incremental_orders`. Its `order_id` primary key and
  `ON CONFLICT(order_id) DO NOTHING` clause make repeated rows safe while still
  allowing other database errors to surface.

### `run_pipeline(...)` and entry point

`run_pipeline` coordinates the tasks:

1. Locate and load all four CSV files.
2. Validate the new-order batch as well as the historical batch.
3. Create the output directory and connect to SQLite.
4. Create/reset demonstration tables.
5. Print the five pipeline stages and inspect the first five orders.
6. Validate historical orders and report calculated amounts and sales.
7. Run ETL and ELT; compare their row counts.
8. Build Bronze/Silver/Gold tables.
9. Run a full load, then seed and update the incremental target.
10. Submit the same valid new batch a second time and check that it inserts
    nothing.
11. Save counts and statuses in `pipeline_run_summary`.
12. Raise an error if the run checks fail; otherwise commit the transaction.

The `if __name__ == "__main__":` block calls `run_pipeline()` when the file is
run directly as `python solution.py`. Importing the module for tests does not
automatically execute the pipeline.

## SQLite table guide

| Table | Contents |
|---|---|
| `bronze_orders` | Original historical order rows from the CSV. |
| `silver_order_details` | Valid historical orders enriched with names and prices. |
| `gold_customer_sales` | One aggregate row per customer: order count and sales. |
| `etl_order_details` | Orders transformed in Python before loading. |
| `elt_raw_orders` | Raw staging orders for the ELT example. |
| `elt_customers`, `elt_products` | Customer and product lookup tables for ELT. |
| `elt_order_details` | Orders transformed by SQLite SQL after raw loading. |
| `full_load_orders` | Current complete historical order file after full load. |
| `incremental_orders` | Persistent target containing unique historical and newly loaded order IDs. |
| `rejected_orders` | Invalid historical or new orders and validation reasons. |
| `pipeline_run_summary` | Extracted, valid, rejected, inserted counts, and run statuses. |

## `tests/test_solution.py`: code walkthrough

The test module uses `unittest`, temporary folders, and in-memory SQLite
databases so most checks do not alter the project's real database.

- **`setUpClass`** reads the provided CSVs once for the test cases.
- **`test_orders_are_transformed_and_sales_are_correct`** checks all historical
  orders are accepted, total sales equal 160400, and customer totals match the
  workbook.
- **`test_invalid_orders_are_rejected`** changes a quantity and customer ID to
  invalid values and confirms that they are rejected.
- **`test_elt_skips_invalid_orders`** verifies the SQL transformation excludes
  an order with quantity zero.
- **`test_incremental_load_does_not_duplicate_orders`** checks new rows insert
  on the first attempt and not on a repeated attempt.
- **`test_full_load_replaces_the_target`** confirms that a new full load replaces
  the previous contents.
- **`test_invalid_new_orders_are_rejected_not_loaded`** creates a temporary
  project copy with an invalid new order and verifies it is rejected and not
  inserted into the incremental target.
- **`test_pipeline_runs_and_saves_expected_results`** runs the pipeline twice
  against a temporary database, checks expected console output and table
  contents, and verifies SQLite's integrity check.

Run these checks with:

```bash
python -m unittest discover -s tests -v
```

## How to run the project

From the project directory:

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
python -m pip install -r requirements.txt
python solution.py
python -m unittest discover -s tests -v
```

The historical data contributes six orders and 160400 in sales. On the first
run with a fresh database, both new rows are inserted. On later runs, the
incremental target is preserved and the same new rows are not duplicated.

## Scope and deployment note

This folder is a local learning project. It demonstrates pipeline concepts
using sample CSV files and SQLite; it is not an Airflow DAG, hosted service, or
Snowflake deployment. It does not configure a scheduler, production alerting,
database backups, or secrets management. The sample data and local database
contain no credentials.
