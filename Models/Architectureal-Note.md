# Retail Data Model

## 1. Architecture

The project loads retail CSV files and transforms them through four layers.

| Layer | Schema | Purpose |
|---|---|---|
| Bronze | `RETAIL_DB.STG` | Stores raw source data and file lineage. |
| Silver | `RETAIL_DB.TMP` | Cleans, validates and standardizes data. Rejected rows go to rejection tables. |
| Warehouse | `RETAIL_DB.TGT` | Stores dimensions and detailed fact tables. |
| Marts | `RETAIL_DB.MARTS` | Stores reporting aggregates. |

```text
CSV files -> STG -> TMP -> TGT -> MARTS -> Reporting
                  |
                  +-> Rejected records
```

The target ERD contains 13 dimensions and 3 detailed facts in `TGT`. The monthly aggregate belongs in `MARTS`.

## 2. Source Files

| File | Cadence | Grain | Main target |
|---|---|---|---|
| `item_master_YYYYMMDD.csv` | Daily | One row per item per snapshot | Product dimensions |
| `store_master_YYYYMMDD.csv` | Weekly | One row per store per snapshot | Location dimensions |
| `pos_sales_YYYYMMDD.csv` | Daily | One row per transaction line | `FCT_SALE` |
| `inventory_snapshot_YYYYMMDD.csv` | Daily | One row per item, store and date | `FCT_INVENTORY` |
| `customer_master.csv` | One-time | One row per customer | `DIM_CUSTOMER` |
| `price_snapshot_YYYYMMDD.csv` | Daily | One row per item, store and date | `FCT_PRICE` |

Customer and price are additional project feeds. The assignment lists the first four source patterns.

## 3. Source-to-Target Mapping

| Source | Target |
|---|---|
| Item master | `DIM_CATEGORY`, `DIM_SUBCATEGORY`, `DIM_BRAND`, `DIM_ITEM` |
| Store master | `DIM_COUNTRY`, `DIM_REGION`, `DIM_STORE` |
| POS sales | `FCT_SALE` |
| Inventory snapshot | `FCT_INVENTORY` |
| Customer master | `DIM_CUSTOMER` |
| Price snapshot | Used to derive price-change periods in `FCT_PRICE` |
| Calendar logic | `DIM_YEAR`, `DIM_QUARTER`, `DIM_MONTH`, `DIM_WEEK`, `DIM_DAY` |
| Sales fact | `AGG_FCT_PRODUCT_SALES_MON` |

Source values are loaded as text in Bronze. Type conversion and validation happen in Silver.

## 4. Warehouse Tables

### Dimensions

| Group | Tables | SCD Type |
|---|---|---|
| Product | `DIM_CATEGORY`, `DIM_SUBCATEGORY`, `DIM_BRAND` | Type 1 |
| Product | `DIM_ITEM` | Type 2 |
| Location | `DIM_COUNTRY`, `DIM_REGION` | Type 1 |
| Location | `DIM_STORE` | Type 2 |
| Customer | `DIM_CUSTOMER` | Type 1 |
| Calendar | `DIM_YEAR`, `DIM_QUARTER`, `DIM_MONTH`, `DIM_WEEK`, `DIM_DAY` | Not SCD-managed |

Product hierarchy: Category -> Subcategory -> Item; Brand also links to Item.

Location hierarchy: Country -> Region -> Store.

SCD Type 1 overwrites changed attributes. SCD Type 2 keeps historical versions using effective dates and a current-row flag.

### Fact Tables

| Table | Grain |
|---|---|
| `FCT_SALE` | One row per transaction line: `TRANSACTION_ID + LINE_NBR`. |
| `FCT_INVENTORY` | One row per item, store and business date. |
| `FCT_PRICE` | One row per item-store price-change event and effective period. |

`FCT_PRICE` is derived from daily price snapshots. It stores price changes, not every repeated daily snapshot.

### Monthly Aggregate

| Table | Grain | Source |
|---|---|---|
| `AGG_FCT_PRODUCT_SALES_MON` | One row per `ITEM_KY + STORE_KY + MONTH_KY`. | `FCT_SALE` |

## 5. Data Quality

The pipeline applies these checks:

- Required business keys must be present.
- Dates and numeric values must convert successfully.
- Duplicate records are removed or rejected according to the source grain.
- Item, store and customer references must resolve.
- Quantities and prices must meet the agreed business rules.
- Fact rows must resolve to the correct dimension versions for their business dates.
- Source, fact and aggregate results must reconcile.

Invalid rows are stored in the relevant rejection tables with their source details and rejection reason.

## 6. Reload and Idempotency

| Layer | Reload approach |
|---|---|
| Bronze | Use file checksum to skip an unchanged file. |
| Silver | Reprocess the relevant source file deterministically; avoid duplicate business keys. |
| Dimensions | Use business-key-based `MERGE`. SCD2 inserts a new version only when tracked attributes change. |
| Sales and inventory facts | Use the natural fact grain to prevent duplicate records during reruns. |
| Price fact | Close the previous period and create a new event only when price, cost or applicable SCD2 keys change. |
| Monthly aggregate | Recompute affected item-store-month groups from `FCT_SALE`. |

## 7. Assumptions

- Customer and price files are project extensions beyond the four source feeds named in the assignment.
- Price history is derived from daily snapshots.
- The initial `DIM_ITEM` effective start date may be inferred from the earliest observed business activity when source history is unavailable. Treat this as an assumption until confirmed by a source rule.
