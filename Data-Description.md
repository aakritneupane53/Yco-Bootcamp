# Retail source data package

## Purpose
Synthetic source data for the retail medallion-architecture assignment. The files are intentionally imperfect so that STG ingestion, TMP cleansing/validation, quarantine, and TGT loading can be demonstrated.

## Test window
Five business dates: 2026-11-10 through 2026-11-14.

## Files and grain
| File pattern | Frequency | Grain | Target use |
|---|---|---|---|
| `item_master_YYYYMMDD.csv` | Daily snapshot | One row per item | Product dimensions: item, brand, category, subcategory |
| `store_master_YYYYMMDD.csv` | Weekly snapshot | One row per store | Location dimensions: store, region, country |
| `pos_sales_YYYYMMDD.csv` | Daily | One row per transaction line | `FCT_SALE` |
| `inventory_snapshot_YYYYMMDD.csv` | Daily snapshot | One row per item/store/business date | `FCT_INVENTORY` |
| `customer_master.csv` | One-time reference | One row per customer | `DIM_CUSTOMER` |
| `price_snapshot_YYYYMMDD.csv` | Daily snapshot | One row per item/store/business date | Derive positional `FCT_PRICE` history |

The customer and price files are proposed supporting feeds; the assignment brief explicitly names only item master, store master, POS sales, and inventory snapshot. This extension is documented as a project assumption.

## Source schemas
- `item_master`: `item_id`, `item_name`, `subcategory_id`, `subcategory_name`, `category_id`, `category_name`, `brand_id`, `brand_name`, `unit_of_measure`, `active_flg`
- `store_master`: `store_id`, `store_name`, `region_id`, `region_name`, `country_id`, `country_name`, `city`, `store_type`, `active_flg`
- `pos_sales`: `transaction_id`, `line_number`, `sale_datetime`, `business_date`, `store_id`, `item_id`, `customer_id`, `sold_qty`, `selling_price_amt`, `discount_amt`, `sales_amt`, `cost_amt`
- `inventory_snapshot`: `business_date`, `item_id`, `store_id`, `on_hand_qty`, `available_qty`, `reserved_qty`, `inventory_value_amt`
- `customer_master`: `customer_id`, `customer_name`, `city`, `loyalty_tier`, `registration_date`, `active_flg`
- `price_snapshot`: `business_date`, `item_id`, `store_id`, `selling_price_amt`, `unit_cost_amt`

All CSV columns are delivered as text in the raw files. Type casting belongs in TMP, not in STG.

## Intentional data-quality scenarios
### Cleanable in TMP
- Leading/trailing whitespace in item names and business keys.
- Inconsistent casing in brand/store attributes.
- Exact duplicate rows.
- Duplicate business keys in master/snapshot files.
- Numeric values represented as strings (normal source behavior).
- Duplicate transaction lines and duplicate item/store/date inventory snapshots.

### Expected quarantine candidates
- Missing business keys.
- Unknown item, store, or customer references.
- Negative sales/inventory quantities.
- Invalid numeric values such as `N/A` or `abc`.
- Invalid business date.
- Negative price.
- Available inventory greater than on-hand inventory.
- Sales amount inconsistent with quantity, price, and discount.
- Amount outside reasonable business limits.

## Suggested TMP rules
1. Trim business keys and descriptive strings; standardize controlled-value casing.
2. Cast numeric and date fields safely; route failed casts to quarantine.
3. Deduplicate exact duplicates.
4. For duplicate business keys with conflicting payloads, do not silently choose a row: apply a documented deterministic rule or quarantine the conflict.
5. Validate foreign keys against cleaned item, store, and customer master datasets.
6. Validate quantity and amount rules, including `sales_amt = sold_qty * selling_price_amt - discount_amt` within an agreed tolerance.
7. For inventory, require `on_hand_qty >= 0`, `reserved_qty >= 0`, `available_qty >= 0`, and `available_qty + reserved_qty = on_hand_qty` for this generated dataset.
8. For price snapshots, require non-negative selling price and unit cost. Compare consecutive daily snapshots to derive price effective periods in `FCT_PRICE`.

## Important assumptions
- The generated five-day period is 2026-11-10 to 2026-11-14.
- The store master is supplied once for the test week.
- Customer attributes are stable during the test period and supplied once.
- Item master is supplied daily, even if no item attributes change.
- Price is represented as a separate daily item/store snapshot feed to support a location-specific positional price fact.
- Date dimensions are generated from calendar logic; monthly aggregate sales are derived from `FCT_SALE`.
- The source data is synthetic and intended for pipeline testing, not real business reporting.

## Suggested file-level control
Use the exact source filename plus file size/checksum (or another stable file identity) in a load-control table. Do not use ingestion date alone as the idempotency key: the same file may be replayed on a different day.
