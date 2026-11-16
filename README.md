# E-Commerce ETL and Reliable Loading Pipeline

A self-contained Day 46 project using Python, Pandas, and SQLite. It demonstrates
all nine workbook tasks: pipeline stages, extraction, validation and
transformation, ETL versus ELT, Bronze/Silver/Gold layers, full and incremental
loads, idempotency, and run monitoring.

## Requirements

- Python 3.10 or newer
- pip

## Run the project

From this directory:

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
python -m pip install -r requirements.txt
python solution.py
```

The script reads the CSV files from `datasets/`, runs every task in order, and
creates or refreshes `output/ecommerce.db`. The full-load, transformation, and
summary tables are refreshed on each run. The incremental target is preserved
between runs, so rerunning the pipeline does not delete its loaded order IDs.
The same new batch is also submitted twice in each run to demonstrate
duplicate-safe loading.

Run the automated checks with:

```bash
python -m unittest discover -s tests -v
```

## Project files

```text
Project-1/
├── datasets/
│   ├── customers.csv
│   ├── new_orders.csv
│   ├── orders.csv
│   └── products.csv
├── output/
│   └── ecommerce.db          # Generated SQLite database
├── tests/
│   └── test_solution.py
├── .gitignore
├── README.md
├── requirements.txt
└── solution.py
```

## Database layers and load behavior

- `bronze_orders` preserves the input order fields without enrichment.
- `silver_order_details` contains validated orders joined to customers and products,
  with calculated order amounts. Invalid rows and their rejection reasons are
  stored in `rejected_orders`.
- `gold_customer_sales` aggregates order count and sales by customer.
- `etl_order_details` is transformed in Python before loading.
- `elt_raw_orders` is loaded first; `elt_order_details` is then transformed with
  a SQLite query.
- `full_load_orders` is replaced by the complete six-order source.
- `incremental_orders` uses `order_id` as a primary key; repeated rows are
  ignored safely.
- `pipeline_run_summary` records the run counts and final statuses.

The database is a local SQLite demonstration, not a Snowflake table or an
orchestrated Airflow DAG. No credentials or external services are required.

## Expected results

On a fresh database, the supplied records produce 6 valid historical orders,
2 new orders, 8 total orders after the incremental load, 4 customer aggregates,
and total sales of 160400. The repeated incremental execution inserts 0 rows.
On later runs, the existing target remains at 8 rows and the already-loaded
batch inserts 0 new rows.

## Deployment scope

This is a local learning project, not a production deployment package. It uses
SQLite, a small local CSV dataset, and console output; it does not include
service hosting, scheduling, production monitoring, backups, or secrets
management.
