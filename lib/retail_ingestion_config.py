
# --------------------------------------------------
# GENERAL CONFIGURATION
# --------------------------------------------------

DATA_DIR = "/opt/airflow/data"

# SQL files are mounted from the project-root sql/ directory
# into every Airflow container at /opt/airflow/sql.
SQL_DIR = "/opt/airflow/sql/silver"

SNOWFLAKE_CONN_ID = "snowflake_default"

DATABASE = "RETAIL_DB"
WAREHOUSE = "RETAIL_WH"

# --------------------------------------------------
# BRONZE / STAGING LAYER
# --------------------------------------------------

BRONZE_SCHEMA = "STG"

STAGE = f"{DATABASE}.{BRONZE_SCHEMA}.STG_RETAIL_SOURCE"
FILE_FORMAT = f"{DATABASE}.{BRONZE_SCHEMA}.FF_RETAIL_CSV"
INGESTION_LOG = f"{DATABASE}.{BRONZE_SCHEMA}.STG_INGESTION_LOG"

# Backward-compatible alias in case an older DAG
# still imports SCHEMA.
SCHEMA = BRONZE_SCHEMA

# --------------------------------------------------
# SILVER LAYER
# --------------------------------------------------

SILVER_SCHEMA = "TMP"

SILVER_LOAD_CONTROL = (
    f"{DATABASE}.{SILVER_SCHEMA}.SILVER_LOAD_CONTROL"
)


# --------------------------------------------------
# SOURCE FILE CONFIGURATION
# --------------------------------------------------

SOURCE_CONFIG = [
    {
        "pattern": r"item_master_[0-9]{8}\.csv",
        "folder": "item_master",
        "table": "STG_ITEM_MASTER",
        "silver_table": "SILVER_ITEM_MASTER",
        "silver_sql_file": "01_load_item_master.sql",
        "columns": [
            "ITEM_ID",
            "ITEM_NAME",
            "SUBCATEGORY_ID",
            "SUBCATEGORY_NAME",
            "CATEGORY_ID",
            "CATEGORY_NAME",
            "BRAND_ID",
            "BRAND_NAME",
            "UNIT_OF_MEASURE",
            "ACTIVE_FLG",
        ],
    },
    {
        "pattern": r"store_master_[0-9]{8}\.csv",
        "folder": "store_master",
        "table": "STG_STORE_MASTER",
        "silver_table": "SILVER_STORE_MASTER",
        "silver_sql_file": "02_load_store_master.sql",
        "columns": [
            "STORE_ID",
            "STORE_NAME",
            "REGION_ID",
            "REGION_NAME",
            "COUNTRY_ID",
            "COUNTRY_NAME",
            "CITY",
            "STORE_TYPE",
            "ACTIVE_FLG",
        ],
    },
    {
        "pattern": r"pos_sales_[0-9]{8}\.csv",
        "folder": "pos_sales",
        "table": "STG_POS_SALES",
        "silver_table": "SILVER_POS_SALES",
        "silver_sql_file": "04_load_pos_sales.sql",
        "columns": [
            "TRANSACTION_ID",
            "LINE_NUMBER",
            "SALE_DATETIME",
            "BUSINESS_DATE",
            "STORE_ID",
            "ITEM_ID",
            "CUSTOMER_ID",
            "SOLD_QTY",
            "SELLING_PRICE_AMT",
            "DISCOUNT_AMT",
            "SALES_AMT",
            "COST_AMT",
        ],
    },
    {
        "pattern": r"inventory_snapshot_[0-9]{8}\.csv",
        "folder": "inventory_snapshot",
        "table": "STG_INVENTORY_SNAPSHOT",
        "silver_table": "SILVER_INVENTORY_SNAPSHOT",
        "silver_sql_file": "05_load_inventory_snapshot.sql",
        "columns": [
            "BUSINESS_DATE",
            "ITEM_ID",
            "STORE_ID",
            "ON_HAND_QTY",
            "AVAILABLE_QTY",
            "RESERVED_QTY",
            "INVENTORY_VALUE_AMT",
        ],
    },
    {
        "pattern": r"price_snapshot_[0-9]{8}\.csv",
        "folder": "price_snapshot",
        "table": "STG_PRICE_SNAPSHOT",
        "silver_table": "SILVER_PRICE_SNAPSHOT",
        "silver_sql_file": "06_load_price_snapshot.sql",
        "columns": [
            "BUSINESS_DATE",
            "ITEM_ID",
            "STORE_ID",
            "SELLING_PRICE_AMT",
            "UNIT_COST_AMT",
        ],
    },
    {
        "pattern": r"customer_master\.csv",
        "folder": "customer_master",
        "table": "STG_CUSTOMER_MASTER",
        "silver_table": "SILVER_CUSTOMER_MASTER",
        "silver_sql_file": "03_load_customer_master.sql",
        "columns": [
            "CUSTOMER_ID",
            "CUSTOMER_NAME",
            "CITY",
            "LOYALTY_TIER",
            "REGISTRATION_DATE",
            "ACTIVE_FLG",
        ],
    },
]
