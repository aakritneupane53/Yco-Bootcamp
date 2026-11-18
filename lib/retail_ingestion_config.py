
# --------------------------------------------------
# GENERAL CONFIGURATION
# --------------------------------------------------

DATA_DIR = "/opt/airflow/data"

SNOWFLAKE_CONN_ID = "snowflake_default"

DATABASE = "RETAIL_DB"
SCHEMA = "STG"

STAGE = f"{DATABASE}.{SCHEMA}.STG_RETAIL_SOURCE"

FILE_FORMAT = f"{DATABASE}.{SCHEMA}.FF_RETAIL_CSV"

INGESTION_LOG = f"{DATABASE}.{SCHEMA}.STG_INGESTION_LOG"


# --------------------------------------------------
# SOURCE FILE CONFIGURATION
# --------------------------------------------------

SOURCE_CONFIG = [
    {
        "pattern": r"item_master_[0-9]{8}\.csv",
        "folder": "item_master",
        "table": "STG_ITEM_MASTER",
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
