import os
import re
import sys
from io import StringIO
from pathlib import Path
from datetime import timedelta

import pendulum

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.trigger_rule import TriggerRule

sys.path.append("/opt/airflow/lib")

from retail_ingestion_config import (
    DATA_DIR,
    SQL_DIR,
    SNOWFLAKE_CONN_ID,
    DATABASE,
    SCHEMA,
    STAGE,
    FILE_FORMAT,
    INGESTION_LOG,
    SILVER_LOAD_CONTROL,
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


def count_pending_silver_files(cursor, config):
    """
    Count distinct Bronze source files without a successful Silver
    load-control record.

    Bronze.SRC_FILE_NM is used for the join rather than the local
    filename, because Snowflake's staged filename metadata can include
    the stage-folder prefix.
    """
    bronze_table = f"{DATABASE}.{SCHEMA}.{config['table']}"

    query = f"""
        SELECT COUNT(DISTINCT B.SRC_FILE_NM)
        FROM {bronze_table} B
        LEFT JOIN {SILVER_LOAD_CONTROL} L
            ON L.SRC_FILE_NM = B.SRC_FILE_NM
           AND L.SOURCE_TABLE = %s
           AND L.TARGET_TABLE = %s
           AND L.LOAD_STATUS = 'SUCCESS'
        WHERE L.CONTROL_ID IS NULL
    """

    cursor.execute(
        query,
        (config["table"], config["silver_table"]),
    )

    result = cursor.fetchone()
    return int(result[0] or 0)


# --------------------------------------------------
# DAG DEFINITION
# --------------------------------------------------

@dag(
    dag_id="retail_stg_ingestion",
    description="Retail CSV ingestion into Bronze followed by Silver transformations",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["retail", "snowflake", "bronze", "silver"],
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
            raise RuntimeError(
                "Snowflake connection test returned no result"
            )

        print("Snowflake connection successful!")
        print(f"Connection details: {result}")

    # --------------------------------------------------
    # TASK 2: INGEST NEW FILES INTO BRONZE
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

                # Check whether this file was already ingested successfully.
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
                    print(f"Already loaded into Bronze: {filename}")
                    continue

                try:
                    # Upload the file to the internal stage.
                    local_file = os.path.join(DATA_DIR, filename)
                    stage_folder = config["folder"]

                    put_sql = (
                        f"PUT file://{local_file} "
                        f"@{STAGE}/{stage_folder}/ "
                        "AUTO_COMPRESS=FALSE "
                        "OVERWRITE=FALSE"
                    )

                    cursor.execute(put_sql)
                    print(f"Uploaded: {filename}")

                    # Build COPY INTO using the source-column configuration.
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

                    # Record successful Bronze ingestion.
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
                        VALUES (
                            %s, %s, 'SUCCESS',
                            CURRENT_TIMESTAMP(), NULL
                        )
                        """,
                        (filename, target_table),
                    )

                    print(f"Successfully ingested: {filename}")

                except Exception as file_error:
                    error_message = str(file_error)[:2000]

                    # Record failed Bronze ingestion.
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
                        VALUES (
                            %s, %s, 'FAILED',
                            CURRENT_TIMESTAMP(), %s
                        )
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
                "One or more files failed during Bronze ingestion:\n"
                + "\n".join(errors)
            )

    # --------------------------------------------------
    # TASK 3: RUN PENDING BRONZE -> SILVER TRANSFORMATIONS
    # --------------------------------------------------

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def run_pending_silver_transformations():
        """
        Execute only those source-specific SQL files for which at least
        one Bronze source file does not yet have a successful
        SILVER_LOAD_CONTROL record.

        ALL_DONE allows this task to process already-ingested Bronze
        files even if a different file failed during the ingestion task.
        The DAG will still show the Bronze ingestion failure.
        """
        if not os.path.isdir(SQL_DIR):
            raise FileNotFoundError(
                f"Silver SQL directory not found: {SQL_DIR}"
            )

        hook = SnowflakeHook(
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            pending_sources = []

            # Identify source tables that have unprocessed Bronze files.
            for config in SOURCE_CONFIG:
                pending_count = count_pending_silver_files(
                    cursor,
                    config,
                )

                if pending_count > 0:
                    pending_sources.append(config)
                    print(
                        f"{config['table']}: "
                        f"{pending_count} Bronze file(s) pending Silver"
                    )

            if not pending_sources:
                print(
                    "No pending Bronze-to-Silver transformations found."
                )
                return

            # Execute each SQL file using the same Snowflake connection
            # for all statements in that file. This is required because
            # the SQL files create and reuse temporary tables.
            for config in pending_sources:
                sql_file = Path(SQL_DIR) / config["silver_sql_file"]

                if not sql_file.is_file():
                    raise FileNotFoundError(
                        f"Silver SQL file not found: {sql_file}"
                    )

                sql_text = sql_file.read_text(encoding="utf-8")

                if not sql_text.strip():
                    raise ValueError(
                        f"Silver SQL file is empty: {sql_file}"
                    )

                print(
                    f"Starting Silver transformation: "
                    f"{config['table']} -> {config['silver_table']}"
                )
                print(f"SQL file: {sql_file}")

                statement_number = 0

                for result_cursor in conn.execute_stream(
                    StringIO(sql_text),
                    remove_comments=True,
                ):
                    statement_number += 1

                    try:
                        if result_cursor.description:
                            rows = result_cursor.fetchall()
                            if rows:
                                print(
                                    f"Result from statement "
                                    f"{statement_number}: {rows}"
                                )
                        else:
                            print(
                                f"Statement {statement_number} completed; "
                                f"rowcount={result_cursor.rowcount}"
                            )
                    finally:
                        result_cursor.close()

                # Confirm that this source table has no files left
                # without a successful Silver load-control record.
                remaining = count_pending_silver_files(
                    cursor,
                    config,
                )

                if remaining > 0:
                    raise RuntimeError(
                        f"Silver transformation for {config['table']} "
                        f"finished but {remaining} Bronze file(s) still "
                        "have no successful SILVER_LOAD_CONTROL record."
                    )

                print(
                    f"Silver transformation completed: "
                    f"{config['silver_table']}"
                )

        finally:
            cursor.close()
            conn.close()

    # --------------------------------------------------
    # TASK DEPENDENCIES
    # --------------------------------------------------

    connection_test = test_snowflake_connection()
    ingestion = ingest_new_files()
    silver_transformations = run_pending_silver_transformations()

    connection_test >> ingestion >> silver_transformations


retail_stg_ingestion()
