# Retail Data Engineering Pipeline

An end-to-end retail data pipeline built with **Apache Airflow, Snowflake, Docker, Python and SQL**. It loads retail CSV data, cleans it, builds a dimensional warehouse, creates a monthly sales aggregate, and runs data-quality checks.

## 1. Project overview

| Area | Details |
|---|---|
| Business domain | Retail: products, stores, customers, sales, inventory and prices |
| Orchestration | Apache Airflow |
| Data warehouse | Snowflake |
| Runtime | Docker |
| Transformation | SQL files executed by Airflow tasks |
| Main database | `RETAIL_DB` |
| Warehouse | `RETAIL_WH` |
| Airflow connection | `snowflake_default` |
| Main DAG | `retail_stg_ingestion` |
| DAG schedule | Every 5 minutes (`*/5 * * * *`) |

## 2. Architecture

```text
Retail CSV files
      |
      v
Bronze: RETAIL_DB.STG
      |
      v
Silver: RETAIL_DB.TMP
      |
      v
Warehouse: RETAIL_DB.TGT
  - Dimensions
  - Facts
      |
      v
Marts: RETAIL_DB.MARTS
  - Monthly product sales aggregate
      |
      v
Reconciliation and validation
```

## 3. Pipeline stages

| Stage | Schema | What it does |
|---|---|---|
| Bronze | `STG` | Loads source CSV files and records file-ingestion details. |
| Silver | `TMP` | Cleans and validates data, removes duplicates, and stores rejected rows. |
| Warehouse | `TGT` | Loads conformed dimensions and business fact tables. |
| Marts | `MARTS` | Builds an aggregate for monthly product sales by store. |
| Reconciliation | SQL checks | Checks key relationships, duplicates, date validity, row counts and aggregate results. |

The Airflow DAG coordinates the stages. Transformation logic is kept in separate SQL files rather than embedded in Python task code.

## 4. Warehouse model

### Dimensions

| Dimension group | Tables |
|---|---|
| Geography and stores | `DIM_COUNTRY`, `DIM_REGION`, `DIM_STORE` |
| Products | `DIM_CATEGORY`, `DIM_SUBCATEGORY`, `DIM_BRAND`, `DIM_ITEM` |
| Customers | `DIM_CUSTOMER` |
| Calendar | `DIM_YEAR`, `DIM_QUARTER`, `DIM_MONTH`, `DIM_WEEK`, `DIM_DAY` |

`DIM_STORE` and `DIM_ITEM` use **SCD Type 2** to retain tracked attribute history. The other dimensions use **SCD Type 1** in this design.

### Fact tables

| Fact table | Grain |
|---|---|
| `FCT_SALE` | One row per transaction line (`TRANSACTION_ID` + `LINE_NBR`). |
| `FCT_INVENTORY` | One row per item, store and business date. |
| `FCT_PRICE` | One row per item-store price event and effective period. |

### Aggregate

| Table | Grain |
|---|---|
| `AGG_FCT_PRODUCT_SALES_MON` | One row per item, store and month (`ITEM_KY` + `STORE_KY` + `MONTH_KY`). |

## 5. Main project folders

```text
project-root/
├── dags/                  # Airflow DAGs
├── sql/
│   ├── bronze/             # Bronze load SQL
│   ├── silver/             # Silver transformation SQL
│   ├── tgt/                # Dimension and fact load SQL
│   ├── marts/              # Aggregate SQL
│   └── reconciliation/     # Warehouse validation SQL
└── README.md
```

The Docker setup mounts the `dags/` and `sql/` folders into the Airflow containers.

## 6. Running the pipeline

1. Start the project's Docker services.
2. Make sure the Airflow connection `snowflake_default` is configured.
3. Confirm the Snowflake database, schemas and warehouse are available.
4. Place the required source CSV files in the configured data directory.
5. Open the Airflow UI and run or enable `retail_stg_ingestion`.
6. Monitor the task graph and review the final validation and reconciliation tasks.

## 7. Data-quality checks

The pipeline includes checks for:

- Duplicate source and fact-table grains.
- Missing or invalid dimension-key lookups.
- SCD Type 2 effective-date ranges and overlapping versions.
- Calendar coverage for source business dates.
- Source-to-fact row and measure reconciliation.
- Price-event periods and active records.
- Monthly aggregate grain and reconciliation.

Rejected source rows are kept in rejection tables for review instead of being loaded as valid Silver records.

## 8. Backfill note

Airflow backfill creates DAG runs for historical schedule intervals. It does **not** automatically select source files by retail business date. The previously reviewed DAG's file discovery scanned the configured data directory, so confirm that the latest implementation filters files and data by the requested interval before describing it as a business-date backfill. A successful Airflow run alone does not prove historical data was correctly reprocessed.

