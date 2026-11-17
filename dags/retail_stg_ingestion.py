
import os
import re
from datetime import timedelta

import pendulum

from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

DATA_DIR = "/opt/airflow/data"

SNOWFLAKE_CONN_ID = "snowflake_default"

DATABASE = "RETAIL_DB"
SCHEMA = "STG"

STAGE = f"{DATABASE}.{SCHEMA}.STG_RETAIL_SOURCE"
FILE_FORMAT = f"{DATABASE}.{SCHEMA}.FF_RETAIL_CSV"
INGESTION_LOG = f"{DATABASE}.{SCHEMA}.STG_INGESTION_LOG"


# --------------------------------------------------
# SOURCE FILE MAPPING
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


# --------------------------------------------------
# HELPER FUNCTIONS
# --------------------------------------------------

def identify_file(filename):

    for config in SOURCE_CONFIG:
        if re.fullmatch(config["pattern"], filename):
            return config

    return None


def sql_string(value):
    """Safely quote a string as a SQL literal."""
    return "'" + str(value).replace("'", "''") + "'"


# --------------------------------------------------
# DAG DEFINITION
# --------------------------------------------------

@dag(
    dag_id="retail_stg_ingestion",
    description="Automatically ingest retail CSV files into Snowflake STG",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["retail", "snowflake", "staging"],
)
def retail_stg_ingestion():

    @task
    def ingest_new_files():

        if not os.path.isdir(DATA_DIR):
            raise FileNotFoundError(
                f"Data directory not found: {DATA_DIR}"
            )

        files = sorted(os.listdir(DATA_DIR))

        # Only process completed CSV files.
        csv_files = [
            filename
            for filename in files
            if filename.lower().endswith(".csv")
            and os.path.isfile(os.path.join(DATA_DIR, filename))
        ]

        if not csv_files:
            print("No CSV files found. Nothing to ingest.")
            return

        hook = SnowflakeHook(
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        conn = hook.get_conn()
        cursor = conn.cursor()

        errors = []

        try:

            for filename in csv_files:

                config = identify_file(filename)

                if config is None:
                    print(f"Unsupported file skipped: {filename}")
                    continue

                target_table = (
                    f"{DATABASE}.{SCHEMA}.{config['table']}"
                )

                print(f"Processing file: {filename}")

                # ----------------------------------
                # 1. CHECK INGESTION LOG
                # ----------------------------------

                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {INGESTION_LOG}
                    WHERE FILE_NAME = %s
                      AND TARGET_TABLE = %s
                      AND LOAD_STATUS = 'SUCCESS'
                    """,
                    (filename, target_table),
                )

                already_loaded = cursor.fetchone()[0] > 0

                if already_loaded:
                    print(f"Already loaded. Skipping: {filename}")
                    continue

                try:

                    # ----------------------------------
                    # 2. UPLOAD FILE TO SNOWFLAKE STAGE
                    # ----------------------------------

                    local_file = os.path.join(
                        DATA_DIR, filename
                    )

                    stage_folder = config["folder"]

                    put_sql = f"""
                    PUT {sql_string("file://" + local_file)}
                    @{STAGE}/{stage_folder}/
                    AUTO_COMPRESS = FALSE
                    OVERWRITE = FALSE
                    """

                    cursor.execute(put_sql)

                    print(f"Uploaded to stage: {filename}")

                    # ----------------------------------
                    # 3. BUILD COPY INTO QUERY
                    # ----------------------------------

                    source_columns = ", ".join(
                        f"${i + 1}"
                        for i in range(len(config["columns"]))
                    )

                    target_columns = ", ".join(
                        config["columns"]
                        + [
                            "SRC_FILE_NM",
                            "ROW_NUM",
                            "INGEST_TS",
                            "BATCH_ID",
                        ]
                    )

                    batch_id = sql_string(
                        os.environ.get(
                            "AIRFLOW_CTX_DAG_RUN_ID",
                            "manual_run",
                        )[:100]
                    )

                    copy_sql = f"""
                    COPY INTO {target_table}
                    ({target_columns})

                    FROM (
                        SELECT
                            {source_columns},
                            METADATA$FILENAME::VARCHAR,
                            METADATA$FILE_ROW_NUMBER::NUMBER,
                            METADATA$START_SCAN_TIME::TIMESTAMP_NTZ,
                            {batch_id}

                        FROM @{STAGE}/{stage_folder}/{filename}
                            (FILE_FORMAT => '{FILE_FORMAT}')
                    )

                    FORCE = FALSE
                    ON_ERROR = 'ABORT_STATEMENT'
                    """

                    cursor.execute(copy_sql)

                    copy_results = cursor.fetchall()

                    print(f"COPY completed for: {filename}")
                    print(f"COPY results: {copy_results}")

                    # ----------------------------------
                    # 4. RECORD SUCCESS
                    # ----------------------------------

                    cursor.execute(
                        f"""
                        INSERT INTO {INGESTION_LOG}
                        (
                            FILE_NAME,
                            TARGET_TABLE,
                            LOAD_STATUS,
                            LOAD_TS,
                            ERROR_MESSAGE
                        )
                        VALUES (%s, %s, %s, CURRENT_TIMESTAMP(), NULL)
                        """,
                        (
                            filename,
                            target_table,
                            "SUCCESS",
                        ),
                    )

                    print(f"SUCCESS: {filename}")

                except Exception as file_error:

                    error_message = str(file_error)[:2000]

                    # ----------------------------------
                    # 5. RECORD FAILURE
                    # ----------------------------------

                    cursor.execute(
                        f"""
                        INSERT INTO {INGESTION_LOG}
                        (
                            FILE_NAME,
                            TARGET_TABLE,
                            LOAD_STATUS,
                            LOAD_TS,
                            ERROR_MESSAGE
                        )
                        VALUES (%s, %s, %s, CURRENT_TIMESTAMP(), %s)
                        """,
                        (
                            filename,
                            target_table,
                            "FAILED",
                            error_message,
                        ),
                    )

                    errors.append(
                        f"{filename}: {error_message}"
                    )

                    print(f"FAILED: {filename}")
                    print(error_message)

        finally:
            cursor.close()
            conn.close()

        if errors:
            raise RuntimeError(
                "One or more files failed:\n" + "\n".join(errors)
            )

    ingest_new_files()


retail_stg_ingestion()
