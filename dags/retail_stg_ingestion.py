
import os
import sys
import re
from datetime import timedelta

import pendulum

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

sys.path.append("/opt/airflow/lib")


from retail_ingestion_config import (
    DATA_DIR,
    SNOWFLAKE_CONN_ID,
    DATABASE,
    SCHEMA,
    STAGE,
    FILE_FORMAT,
    INGESTION_LOG,
    SOURCE_CONFIG,
)


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
    description="Retail CSV ingestion into Snowflake STG",
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

    # --------------------------------------------------
    # TASK 1: TEST SNOWFLAKE CONNECTION
    # --------------------------------------------------

    @task
    def test_snowflake_connection():

        hook = SnowflakeHook(
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        result = hook.get_first("""
            SELECT
                CURRENT_USER(),
                CURRENT_ROLE(),
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                CURRENT_WAREHOUSE(),
                CURRENT_VERSION()
        """)

        if not result:
            raise RuntimeError("Snowflake connection test returned no result")

        print("Snowflake connection successful!")
        print(f"Connection details: {result}")

    # --------------------------------------------------
    # TASK 2: INGEST NEW FILES
    # --------------------------------------------------

    @task
    def ingest_new_files():

        if not os.path.isdir(DATA_DIR):
            raise FileNotFoundError(
                f"Data directory not found: {DATA_DIR}"
            )

        csv_files = sorted([
            filename
            for filename in os.listdir(DATA_DIR)
            if filename.lower().endswith(".csv")
            and os.path.isfile(os.path.join(DATA_DIR, filename))
        ])

        if not csv_files:
            print("No CSV files found. Nothing to ingest.")
            return

        hook = SnowflakeHook(
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        context = get_current_context()
        batch_id = context["run_id"][:100]

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

                print(f"Processing: {filename}")

                # ----------------------------------
                # CHECK INGESTION LOG
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

                if cursor.fetchone()[0] > 0:
                    print(f"Already loaded: {filename}")
                    continue

                try:

                    # ----------------------------------
                    # UPLOAD FILE TO INTERNAL STAGE
                    # ----------------------------------

                    local_file = os.path.join(
                        DATA_DIR, filename
                    )

                    stage_folder = config["folder"]

                    put_sql = (
                        f"PUT file://{local_file} "
                        f"@{STAGE}/{stage_folder}/ "
                        "AUTO_COMPRESS=FALSE "
                        "OVERWRITE=FALSE"
                    )

                    cursor.execute(put_sql)

                    print(f"Uploaded: {filename}")

                    # ----------------------------------
                    # BUILD COPY INTO QUERY
                    # ----------------------------------

                    source_columns = ", ".join(
                        f"${i + 1}"
                        for i in range(len(config["columns"]))
                    )

                    target_columns = ", ".join(
                        config["columns"] + [
                            "SRC_FILE_NM",
                            "ROW_NUM",
                            "INGEST_TS",
                            "BATCH_ID",
                        ]
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
                            {sql_string(batch_id)}
                        FROM @{STAGE}/{stage_folder}/{filename}
                            (FILE_FORMAT => '{FILE_FORMAT}')
                    )
                    FORCE = FALSE
                    ON_ERROR = 'ABORT_STATEMENT'
                    """

                    cursor.execute(copy_sql)

                    copy_results = cursor.fetchall()

                    print(f"COPY results: {copy_results}")

                    # ----------------------------------
                    # RECORD SUCCESS
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
                        VALUES (%s, %s, 'SUCCESS',
                                CURRENT_TIMESTAMP(), NULL)
                        """,
                        (filename, target_table),
                    )

                    print(f"Successfully ingested: {filename}")

                except Exception as file_error:

                    error_message = str(file_error)[:2000]

                    # ----------------------------------
                    # RECORD FAILURE
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
                        VALUES (%s, %s, 'FAILED',
                                CURRENT_TIMESTAMP(), %s)
                        """,
                        (filename, target_table, error_message),
                    )

                    errors.append(
                        f"{filename}: {error_message}"
                    )

                    print(f"Failed: {filename}")
                    print(error_message)

        finally:
            cursor.close()
            conn.close()

        if errors:
            raise RuntimeError(
                "One or more files failed:\n"
                + "\n".join(errors)
            )


    connection_test = test_snowflake_connection()

    ingestion = ingest_new_files()

    connection_test >> ingestion


retail_stg_ingestion()
