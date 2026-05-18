# ELT Pipeline with dbt, Snowflake & Apache Airflow

A production-style ELT (Extract, Load, Transform) pipeline built using **dbt**, **Snowflake**, and **Apache Airflow**. This project demonstrates modern data engineering practices including modular data modeling, reusable macros, automated data quality testing, and pipeline orchestration.

## Architecture

```
┌──────────────────────┐     ┌──────────────────────┐     ┌──────────────────────┐
│                      │     │                      │     │                      │
│   Snowflake          │────▶│   dbt                │────▶│   Apache Airflow     │
│   (Data Warehouse)   │     │   (Transformation)   │     │   (Orchestration)    │
│                      │     │                      │     │                      │
└──────────────────────┘     └──────────────────────┘     └──────────────────────┘
```

**Data flows through three layers:**

```
Source (TPCH Sample Data)
    │
    ▼
Staging Layer (Views)
    ├── stg_tpch_orders
    └── stg_tpch_line_items
    │
    ▼
Intermediate Layer (Tables)
    ├── int_order_items
    └── int_order_items_summary
    │
    ▼
Mart Layer (Tables)
    └── fct_orders
```

## Tech Stack

| Tool | Purpose |
|------|---------|
| **Snowflake** | Cloud data warehouse — stores raw and transformed data |
| **dbt (Data Build Tool)** | SQL-based data transformation, testing, and documentation |
| **Apache Airflow** | Workflow orchestration and scheduling |
| **Astronomer Cosmos** | Seamless integration between Airflow and dbt |
| **Docker** | Containerized Airflow deployment via Astro CLI |

## Project Structure

```
dbt_airflow/
│
├── data_pipeline/                  # dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   ├── tpch_sources.yml        # Source definitions (TPCH dataset)
│   │   │   ├── stg_tpch_orders.sql     # Staging: orders
│   │   │   └── stg_tpch_line_items.sql # Staging: line items
│   │   └── marts/
│   │       ├── int_order_items.sql          # Intermediate: order-line item join
│   │       ├── int_order_items_summary.sql  # Intermediate: aggregated summary
│   │       ├── fct_orders.sql               # Fact table: final orders model
│   │       └── generic_tests.yml            # Generic test definitions
│   ├── macros/
│   │   └── pricing.sql             # Reusable macro for discount calculation
│   ├── tests/
│   │   ├── fct_orders_discount.sql  # Singular test: discount validation
│   │   └── fct_orders_date_valid.sql # Singular test: date range validation
│   ├── dbt_project.yml
│   └── packages.yml                 # dbt packages (dbt_utils)
│
├── dbt-dag/                         # Astro/Airflow project
│   ├── dags/
│   │   ├── dbt_dag.py               # Airflow DAG for orchestrating dbt
│   │   └── dbt/
│   │       └── data_pipeline/       # dbt project (copied for Airflow)
│   ├── Dockerfile
│   └── requirements.txt
│
└── README.md
```

## Data Source

This project uses Snowflake's built-in **TPC-H sample dataset** (`snowflake_sample_data.tpch_sf1`), a standard benchmarking dataset that includes:

- **orders** — 1.5M rows of customer orders
- **lineitem** — 6M rows of order line items

No external data download is required — the dataset comes pre-loaded with every Snowflake account.

## Data Modeling

### Staging Layer
Raw source data is cleaned and renamed for consistency. Staging models are materialized as **views** for cost efficiency.

- `stg_tpch_orders` — Renames and selects key columns from the orders table
- `stg_tpch_line_items` — Renames columns and generates a surrogate key using `dbt_utils.generate_surrogate_key`

### Intermediate Layer
Business logic joins and aggregations happen here. These are materialized as **tables**.

- `int_order_items` — Joins orders with line items and applies the discount macro
- `int_order_items_summary` — Aggregates gross sales and discount amounts per order

### Mart Layer
Final fact table ready for analytics and BI consumption.

- `fct_orders` — Combines order details with aggregated sales metrics

## Macros

**`discounted_amount`** — A reusable Jinja macro that calculates discount amounts, following the DRY (Don't Repeat Yourself) principle:

```sql
{% macro discounted_amount(extended_price, discount_percentage, scale=2) %}
    (-1 * {{extended_price}} * {{discount_percentage}})::decimal(16, {{ scale }})
{% endmacro %}
```

## Testing

### Generic Tests
Defined in `generic_tests.yml` on the `fct_orders` model:
- **unique** and **not_null** on `order_key`
- **relationships** test ensuring referential integrity with staging orders
- **accepted_values** on `status_code` (P, O, F)

### Singular Tests
Custom SQL-based data quality checks:
- `fct_orders_discount.sql` — Validates that discount amounts are never positive
- `fct_orders_date_valid.sql` — Validates that order dates fall within a reasonable range (1990 to present)

## Setup & Installation

### Prerequisites
- Python 3.10+
- Snowflake account (free trial works)
- Docker Desktop
- Astro CLI

### 1. Snowflake Setup

Run in a Snowflake SQL worksheet:

```sql
use role accountadmin;

create warehouse dbt_wh with warehouse_size='x-small';
create database if not exists dbt_db;
create role if not exists dbt_role;

grant role dbt_role to user <YOUR_USERNAME>;
grant usage on warehouse dbt_wh to role dbt_role;
grant all on database dbt_db to role dbt_role;

use role dbt_role;
create schema if not exists dbt_db.dbt_schema;
```

### 2. dbt Setup

```bash
python -m venv dbt_env
source dbt_env/Scripts/activate    # Windows
# source dbt_env/bin/activate      # Mac/Linux

pip install dbt-snowflake
cd data_pipeline
dbt deps
dbt debug    # Verify connection
dbt run      # Build all models
dbt test     # Run all tests
```

### 3. Airflow Deployment

```bash
cd dbt-dag
astro dev start
```

Then in the Airflow UI (http://localhost:8080, login `admin`/`admin`):
1. Go to **Admin → Connections**
2. Add a new Snowflake connection with ID `snowflake_conn`
3. Toggle on and trigger the `dbt_dag`

## dbt Commands Reference

| Command | Description |
|---------|-------------|
| `dbt debug` | Test database connection |
| `dbt deps` | Install dbt packages |
| `dbt run` | Execute all models |
| `dbt test` | Run all generic and singular tests |
| `dbt run -s <model_name>` | Run a specific model |
| `dbt test -s <model_name>` | Test a specific model |

## Key Concepts Demonstrated

- **ELT vs ETL** — Transformations happen inside the warehouse, leveraging Snowflake's compute
- **Layered data modeling** — Staging → Intermediate → Mart pattern for maintainability
- **Surrogate keys** — Generated using `dbt_utils.generate_surrogate_key`
- **Reusable macros** — DRY principle applied to SQL transformations
- **Data quality testing** — Both generic (schema-level) and singular (custom SQL) tests
- **RBAC in Snowflake** — Dedicated warehouse, database, role, and schema
- **Pipeline orchestration** — Airflow DAG using Astronomer Cosmos for dbt integration

## Cleanup

To tear down Snowflake resources:

```sql
use role accountadmin;
drop warehouse if exists dbt_wh;
drop database if exists dbt_db;
drop role if exists dbt_role;
```

To stop Airflow:

```bash
astro dev stop
```

