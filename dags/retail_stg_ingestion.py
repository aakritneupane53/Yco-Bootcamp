import hashlib
import os
import re
import sys
import time
from datetime import timedelta
from io import StringIO
from pathlib import Path

import pendulum

# Airflow SDK imports
from airflow.sdk import dag, task, get_current_context, PokeReturnValue
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

sys.path.append("/opt/airflow/lib")

from retail_ingestion_config import (
    DATA_DIR,
    SQL_DIR,
    SNOWFLAKE_CONN_ID,
    DATABASE,
    WAREHOUSE,
    BRONZE_SCHEMA,
    STAGE,
    FILE_FORMAT,
    INGESTION_LOG,
    SILVER_LOAD_CONTROL,
    SOURCE_CONFIG,
)


# --------------------------------------------------
# FILE ARRIVAL SENSOR SETTINGS
# --------------------------------------------------
# The sensor waits for at least one stable CSV file for EVERY source type
# declared in SOURCE_CONFIG. It does not check whether files are new or
# previously ingested: existing files must satisfy the arrival barrier too.
# Ingestion idempotency is handled separately by the checksum log.
EXPECTED_SOURCE_TABLES = {item["table"] for item in SOURCE_CONFIG}
FILE_SENSOR_POKE_INTERVAL_SECONDS = 30
FILE_SENSOR_TIMEOUT_SECONDS = 60 * 60  # 1 hour
FILE_STABILITY_SECONDS = 30


# --------------------------------------------------
# HELPERS
# --------------------------------------------------

def identify_file(filename):
    """Return the source configuration matching a local CSV filename."""
    for config in SOURCE_CONFIG:
        if re.fullmatch(config["pattern"], filename, flags=re.IGNORECASE):
            return config
    return None


def sql_string(value):
    """Quote a value used in a Snowflake SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def calculate_file_identity(file_path):
    """
    Calculate SHA-256 (our stable file identity), MD5 (for matching files
    uploaded by the previous version of this DAG), and byte size.

    The stat check avoids ingesting a file while another process is still
    writing to it.
    """
    path = Path(file_path)
    stat_before = path.stat()

    sha256 = hashlib.sha256()
    md5 = hashlib.md5()
    byte_count = 0

    with path.open("rb") as source_file:
        while True:
            chunk = source_file.read(1024 * 1024)
            if not chunk:
                break
            sha256.update(chunk)
            md5.update(chunk)
            byte_count += len(chunk)

    stat_after = path.stat()

    if (
        stat_before.st_size != stat_after.st_size
        or stat_before.st_mtime_ns != stat_after.st_mtime_ns
        or byte_count != stat_after.st_size
    ):
        raise RuntimeError(
            f"File changed while its checksum was being calculated: {path.name}"
        )

    return {
        "sha256": sha256.hexdigest(),
        "md5": md5.hexdigest(),
        "size_bytes": byte_count,
    }


def ensure_ingestion_log_columns(cursor):
    """
    Backward-compatible migration for the existing filename-only log table.
    These statements are safe to run on every DAG execution.
    """
    cursor.execute(
        f"""
        ALTER TABLE {INGESTION_LOG}
        ADD COLUMN IF NOT EXISTS FILE_CHECKSUM VARCHAR(64)
        """
    )
    cursor.execute(
        f"""
        ALTER TABLE {INGESTION_LOG}
        ADD COLUMN IF NOT EXISTS FILE_SIZE_BYTES NUMBER(38,0)
        """
    )


def legacy_stage_file_matches(cursor, config, filename, file_identity):
    """
    Existing successful log rows have no checksum. Before treating such a
    file as new, compare the local file against the original stage object
    created by the old DAG (folder/filename).

    If the old staged object's MD5 and size match, backfill the log row and
    skip the file. If it differs, the local file is a new version.
    """
    target_table = f"{DATABASE}.{BRONZE_SCHEMA}.{config['table']}"

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM {INGESTION_LOG}
        WHERE FILE_NAME = %s
          AND TARGET_TABLE = %s
          AND LOAD_STATUS = 'SUCCESS'
          AND FILE_CHECKSUM IS NULL
        """,
        (filename, target_table),
    )

    if int(cursor.fetchone()[0] or 0) == 0:
        return False

    old_stage_path = f"@{STAGE}/{config['folder']}/{filename}"

    try:
        cursor.execute(f"LIST {old_stage_path}")
        staged_files = cursor.fetchall()
    except Exception as list_error:
        print(
            f"Could not inspect legacy stage object for {filename}: "
            f"{list_error}"
        )
        return False

    for staged_file in staged_files:
        # Snowflake LIST columns: name, size, md5, last_modified, ...
        staged_name = str(staged_file[0])
        staged_size = int(staged_file[1])
        staged_md5 = str(staged_file[2] or "").lower()

        if (
            staged_name.endswith(filename)
            and staged_size == file_identity["size_bytes"]
            and staged_md5 == file_identity["md5"]
        ):
            cursor.execute(
                f"""
                UPDATE {INGESTION_LOG}
                SET FILE_CHECKSUM = %s,
                    FILE_SIZE_BYTES = %s
                WHERE FILE_NAME = %s
                  AND TARGET_TABLE = %s
                  AND LOAD_STATUS = 'SUCCESS'
                  AND FILE_CHECKSUM IS NULL
                """,
                (
                    file_identity["sha256"],
                    file_identity["size_bytes"],
                    filename,
                    target_table,
                ),
            )
            print(
                f"Legacy file is unchanged; checksum backfilled and skipped: "
                f"{filename}"
            )
            return True

    return False


def count_pending_files(cursor, config):
    """
    A Bronze source file is pending if its latest Silver control record is
    absent, failed, started, or incomplete. A historical SUCCESS does not
    hide a later FAILED execution.
    """
    bronze_table = (
        f"{DATABASE}.{BRONZE_SCHEMA}.{config['table']}"
    )

    query = f"""
        WITH LATEST_CONTROL AS (
            SELECT
                SRC_FILE_NM,
                CONTROL_ID,
                LOAD_STATUS,
                COMPLETED_TS,
                ROW_NUMBER() OVER (
                    PARTITION BY SRC_FILE_NM
                    ORDER BY
                        COALESCE(COMPLETED_TS, STARTED_TS) DESC NULLS LAST,
                        STARTED_TS DESC NULLS LAST,
                        CONTROL_ID DESC
                ) AS RN
            FROM {SILVER_LOAD_CONTROL}
            WHERE SOURCE_TABLE = %s
              AND TARGET_TABLE = %s
        )
        SELECT DISTINCT B.SRC_FILE_NM
        FROM {bronze_table} B
        LEFT JOIN LATEST_CONTROL L
            ON L.SRC_FILE_NM = B.SRC_FILE_NM
           AND L.RN = 1
        WHERE B.SRC_FILE_NM IS NOT NULL
          AND (
              L.CONTROL_ID IS NULL
              OR L.LOAD_STATUS <> 'SUCCESS'
              OR L.COMPLETED_TS IS NULL
          )
        ORDER BY B.SRC_FILE_NM
    """

    cursor.execute(
        query,
        (config["table"], config["silver_table"]),
    )
    return [row[0] for row in cursor.fetchall()]


def record_silver_failure(
    cursor,
    config,
    source_file_names,
    airflow_run_id,
    error_message,
):
    """Persist a failed audit state so the file remains pending next run."""
    bronze_table = (
        f"{DATABASE}.{BRONZE_SCHEMA}.{config['table']}"
    )
    message = str(error_message)[:4000]

    for source_file_name in source_file_names:
        cursor.execute(
            f"""
            UPDATE {SILVER_LOAD_CONTROL}
            SET LOAD_STATUS = 'FAILED',
                AIRFLOW_RUN_ID = %s,
                COMPLETED_TS = CURRENT_TIMESTAMP(),
                ERROR_MESSAGE = %s
            WHERE SOURCE_TABLE = %s
              AND TARGET_TABLE = %s
              AND SRC_FILE_NM = %s
              AND LOAD_STATUS IN ('STARTED', 'FAILED')
            """,
            (
                airflow_run_id,
                message,
                config["table"],
                config["silver_table"],
                source_file_name,
            ),
        )

        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM {SILVER_LOAD_CONTROL}
            WHERE SOURCE_TABLE = %s
              AND TARGET_TABLE = %s
              AND SRC_FILE_NM = %s
            """,
            (
                config["table"],
                config["silver_table"],
                source_file_name,
            ),
        )

        if int(cursor.fetchone()[0] or 0) == 0:
            cursor.execute(
                f"""
                INSERT INTO {SILVER_LOAD_CONTROL} (
                    CONTROL_ID,
                    SOURCE_TABLE,
                    TARGET_TABLE,
                    SRC_FILE_NM,
                    BATCH_ID,
                    AIRFLOW_RUN_ID,
                    LOAD_STATUS,
                    STARTED_TS,
                    COMPLETED_TS,
                    ROWS_READ,
                    ROWS_LOADED,
                    ROWS_QUARANTINED,
                    ERROR_MESSAGE
                )
                SELECT
                    UUID_STRING(),
                    %s,
                    %s,
                    %s,
                    MAX(BATCH_ID),
                    %s,
                    'FAILED',
                    CURRENT_TIMESTAMP(),
                    CURRENT_TIMESTAMP(),
                    COUNT(DISTINCT ROW_NUM),
                    0,
                    0,
                    %s
                FROM {bronze_table}
                WHERE SRC_FILE_NM = %s
                """,
                (
                    config["table"],
                    config["silver_table"],
                    source_file_name,
                    airflow_run_id,
                    message,
                    source_file_name,
                ),
            )


# --------------------------------------------------
# DAG
# --------------------------------------------------

@dag(
    dag_id="retail_stg_ingestion",
    description=(
        "Wait for the required retail CSV batch, dynamically ingest each file "
        "into Bronze in parallel, process pending files into Silver, and audit results"
    ),
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["retail", "snowflake", "sensors", "dynamic-mapping", "bronze", "silver"],
)
def retail_stg_ingestion():

    # --------------------------------------------------
    # 1. TEST SNOWFLAKE CONNECTION
    # --------------------------------------------------

    @task
    def test_snowflake_connection():
        hook = SnowflakeHook(
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute(f"USE WAREHOUSE {WAREHOUSE}")
            cursor.execute(
                """
                SELECT
                    CURRENT_USER(),
                    CURRENT_ROLE(),
                    CURRENT_DATABASE(),
                    CURRENT_SCHEMA(),
                    CURRENT_WAREHOUSE(),
                    CURRENT_VERSION()
                """
            )
            result = cursor.fetchone()

            if not result:
                raise RuntimeError(
                    "Snowflake connection test returned no result."
                )

            if result[4] is None:
                raise RuntimeError(
                    f"Snowflake warehouse {WAREHOUSE} is not active."
                )

            print("Snowflake connection successful.")
            print(f"Connection details: {result}")

        finally:
            cursor.close()
            conn.close()

    # --------------------------------------------------
    # 2. WAIT FOR THE REQUIRED FILE ARRIVAL BATCH
    # --------------------------------------------------

    @task.sensor(
        task_id="wait_for_required_files",
        poke_interval=FILE_SENSOR_POKE_INTERVAL_SECONDS,
        timeout=FILE_SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
    )
    def wait_for_required_files():
        """Wait for one stable local CSV file for every configured source type.

        Existing files count, whether or not they were ingested in a previous
        DAG run. The sensor only checks local file presence/stability; the
        ingestion task later uses checksums to skip unchanged files.
        """
        data_path = Path(DATA_DIR)
        if not data_path.is_dir():
            print(f"Waiting for data directory to become available: {DATA_DIR}")
            return PokeReturnValue(is_done=False)

        now = time.time()
        stable_by_source = {source_table: [] for source_table in EXPECTED_SOURCE_TABLES}
        unstable_by_source = {source_table: [] for source_table in EXPECTED_SOURCE_TABLES}

        for file_path in sorted(data_path.iterdir()):
            if not file_path.is_file() or file_path.suffix.lower() != ".csv":
                continue

            config = identify_file(file_path.name)
            if config is None:
                print(f"Ignoring unsupported CSV while sensing: {file_path.name}")
                continue

            source_table = config["table"]
            try:
                stat = file_path.stat()
            except FileNotFoundError:
                # A producer may still be moving/renaming the file.
                continue

            if stat.st_size <= 0:
                unstable_by_source[source_table].append(
                    f"{file_path.name} (empty)"
                )
                continue

            age_seconds = now - stat.st_mtime
            if age_seconds < FILE_STABILITY_SECONDS:
                unstable_by_source[source_table].append(
                    f"{file_path.name} ({age_seconds:.1f}s old)"
                )
                continue

            try:
                # Confirms the file did not change while it was being read.
                calculate_file_identity(file_path)
            except (FileNotFoundError, RuntimeError) as file_error:
                unstable_by_source[source_table].append(
                    f"{file_path.name} ({file_error})"
                )
                continue

            stable_by_source[source_table].append(file_path.name)

        missing_sources = sorted(
            source_table
            for source_table, filenames in stable_by_source.items()
            if not filenames
        )

        if missing_sources:
            details = []
            for source_table in missing_sources:
                config = next(
                    item for item in SOURCE_CONFIG
                    if item["table"] == source_table
                )
                details.append(
                    f"{source_table} pattern={config['pattern']!r}; "
                    f"unstable={unstable_by_source[source_table]}"
                )
            print(
                "Waiting for at least one stable file for every configured "
                f"CSV type. Missing types: {' | '.join(details)}"
            )
            return PokeReturnValue(is_done=False)

        # Map every stable supported file currently present, not just one file
        # per type. This permits multiple dated snapshots/files for a source.
        ready_file_names = sorted(
            filename
            for filenames in stable_by_source.values()
            for filename in filenames
        )
        print(
            "File arrival barrier passed. Stable files by source type: "
            + str(stable_by_source)
        )
        return PokeReturnValue(
            is_done=True,
            xcom_value=ready_file_names,
        )

    # --------------------------------------------------
    # 3. PREPARE THE INGESTION LOG ONCE BEFORE PARALLEL TASKS
    # --------------------------------------------------

    @task
    def prepare_ingestion_log():
        """Apply the backward-compatible log-column migration once."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(f"USE WAREHOUSE {WAREHOUSE}")
            ensure_ingestion_log_columns(cursor)
            print(f"Ingestion log is ready: {INGESTION_LOG}")
        finally:
            cursor.close()
            conn.close()

    # --------------------------------------------------
    # 4. DISCOVER FILES TO BE DYNAMICALLY MAPPED
    # --------------------------------------------------

    @task
    def discover_files_for_mapping(sensor_file_names):
        """Return one mapping payload per ready supported CSV file."""
        data_path = Path(DATA_DIR)
        file_infos = []

        for filename in sensor_file_names:
            file_path = data_path / filename
            if not file_path.is_file():
                raise FileNotFoundError(
                    f"File disappeared after sensor completed: {file_path}"
                )

            config = identify_file(filename)
            if config is None:
                # The file may have changed between the sensor and discovery.
                print(f"Unsupported file ignored after sensor: {filename}")
                continue

            file_infos.append({
                "filename": filename,
                "file_path": str(file_path),
                "source_table": config["table"],
            })

        covered_source_tables = {item["source_table"] for item in file_infos}
        missing_source_tables = sorted(EXPECTED_SOURCE_TABLES - covered_source_tables)
        if missing_source_tables:
            raise RuntimeError(
                "The file set changed after the sensor passed. Missing at least "
                "one file for these configured source types: "
                + ", ".join(missing_source_tables)
            )

        print(
            f"Creating {len(file_infos)} dynamically mapped ingestion task(s): "
            f"{[item['filename'] for item in file_infos]}"
        )
        return file_infos

    # --------------------------------------------------
    # 5. ONE BRONZE INGESTION TASK PER FILE (DYNAMIC MAPPING)
    # --------------------------------------------------

    @task(
        task_id="ingest_single_file",
        retries=1,
        retry_delay=timedelta(minutes=2),
    )
    def ingest_single_file(file_info):
        """Ingest exactly one CSV file. Airflow maps this task per file."""
        filename = file_info["filename"]
        file_path = Path(file_info["file_path"])
        config = identify_file(filename)

        if config is None:
            return {
                "status": "UNSUPPORTED",
                "filename": filename,
                "error": "No SOURCE_CONFIG pattern matched this file.",
            }

        if not file_path.is_file():
            return {
                "status": "FAILED",
                "filename": filename,
                "target_table": config["table"],
                "error": f"Local file no longer exists: {file_path}",
            }

        # Recheck stability immediately before reading/uploading the file.
        current_age = time.time() - file_path.stat().st_mtime
        if current_age < FILE_STABILITY_SECONDS:
            return {
                "status": "FAILED",
                "filename": filename,
                "target_table": config["table"],
                "error": (
                    f"File was modified after the sensor check and is not yet "
                    f"stable ({current_age:.1f}s old)."
                ),
            }

        context = get_current_context()
        batch_id = context["run_id"][:100]
        target_table = f"{DATABASE}.{BRONZE_SCHEMA}.{config['table']}"

        file_identity = None
        conn = None
        cursor = None
        result = None

        try:
            file_identity = calculate_file_identity(file_path)
            if file_identity["size_bytes"] == 0:
                raise ValueError(
                    "Empty CSV file detected; ingestion was not attempted."
                )

            hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
            conn = hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(f"USE WAREHOUSE {WAREHOUSE}")

            # Same filename + same bytes is already ingested.
            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {INGESTION_LOG}
                WHERE FILE_NAME = %s
                  AND TARGET_TABLE = %s
                  AND FILE_CHECKSUM = %s
                  AND LOAD_STATUS = 'SUCCESS'
                """,
                (filename, target_table, file_identity["sha256"]),
            )
            if int(cursor.fetchone()[0] or 0) > 0:
                print(f"Already ingested (same checksum): {filename}")
                return {
                    "status": "ALREADY_INGESTED",
                    "filename": filename,
                    "target_table": config["table"],
                    "checksum": file_identity["sha256"],
                    "size_bytes": file_identity["size_bytes"],
                }

            # Backward compatibility for successful rows created before
            # checksum columns were introduced.
            if legacy_stage_file_matches(
                cursor, config, filename, file_identity
            ):
                return {
                    "status": "ALREADY_INGESTED",
                    "filename": filename,
                    "target_table": config["table"],
                    "checksum": file_identity["sha256"],
                    "size_bytes": file_identity["size_bytes"],
                }

            filename_stem = filename.rsplit(".", 1)[0]
            stage_version_dir = (
                f"{config['folder']}/{filename_stem}/"
                f"{file_identity['sha256']}"
            )

            put_sql = (
                f"PUT {sql_string('file://' + str(file_path.resolve()))} "
                f"@{STAGE}/{stage_version_dir}/ "
                "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            cursor.execute(put_sql)
            put_results = cursor.fetchall()
            print(f"PUT results for {filename}: {put_results}")

            source_columns = ", ".join(
                f"${index + 1}" for index in range(len(config["columns"]))
            )
            target_columns = ", ".join(
                config["columns"] + [
                    "SRC_FILE_NM", "ROW_NUM", "INGEST_TS", "BATCH_ID"
                ]
            )
            staged_file_path = f"@{STAGE}/{stage_version_dir}/{filename}"

            copy_sql = f"""
                COPY INTO {target_table} ({target_columns})
                FROM (
                    SELECT
                        {source_columns},
                        METADATA$FILENAME::VARCHAR,
                        METADATA$FILE_ROW_NUMBER::NUMBER,
                        METADATA$START_SCAN_TIME::TIMESTAMP_NTZ,
                        {sql_string(batch_id)}
                    FROM {staged_file_path}
                        (FILE_FORMAT => '{FILE_FORMAT}')
                )
                FORCE = FALSE
                ON_ERROR = 'ABORT_STATEMENT'
            """
            cursor.execute(copy_sql)
            copy_results = cursor.fetchall()
            print(f"COPY results for {filename}: {copy_results}")

            failed_copy_rows = [
                row for row in copy_results
                if len(row) > 1 and str(row[1]).upper() in {
                    "LOAD_FAILED", "PARTIALLY_LOADED"
                }
            ]
            if failed_copy_rows:
                raise RuntimeError(
                    f"COPY reported failed rows: {failed_copy_rows}"
                )

            cursor.execute(
                f"""
                INSERT INTO {INGESTION_LOG} (
                    FILE_NAME, TARGET_TABLE, LOAD_STATUS, LOAD_TS,
                    ERROR_MESSAGE, FILE_CHECKSUM, FILE_SIZE_BYTES
                )
                VALUES (%s, %s, 'SUCCESS', CURRENT_TIMESTAMP(), NULL, %s, %s)
                """,
                (
                    filename,
                    target_table,
                    file_identity["sha256"],
                    file_identity["size_bytes"],
                ),
            )

            result = {
                "status": "INGESTED",
                "filename": filename,
                "target_table": config["table"],
                "checksum": file_identity["sha256"],
                "size_bytes": file_identity["size_bytes"],
            }
            print(f"Successfully ingested {filename} into {target_table}")

        except Exception as file_error:
            error_message = str(file_error)[:2000]
            print(f"Failed to ingest {filename}: {error_message}")

            # Persist failure for this file. The next DAG run retries it.
            if cursor is not None:
                try:
                    checksum = (
                        file_identity["sha256"] if file_identity else None
                    )
                    size_bytes = (
                        file_identity["size_bytes"] if file_identity else None
                    )
                    cursor.execute(
                        f"""
                        INSERT INTO {INGESTION_LOG} (
                            FILE_NAME, TARGET_TABLE, LOAD_STATUS, LOAD_TS,
                            ERROR_MESSAGE, FILE_CHECKSUM, FILE_SIZE_BYTES
                        )
                        VALUES (%s, %s, 'FAILED', CURRENT_TIMESTAMP(), %s, %s, %s)
                        """,
                        (
                            filename, target_table, error_message,
                            checksum, size_bytes,
                        ),
                    )
                except Exception as log_error:
                    error_message += (
                        f" | Could not write failure log: {log_error}"
                    )

            result = {
                "status": "FAILED",
                "filename": filename,
                "target_table": config["table"],
                "error": error_message,
            }

        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

        return result

    # --------------------------------------------------
    # 6. COLLECT RESULTS FROM THE MAPPED INGESTION TASKS
    # --------------------------------------------------

    @task
    def summarize_ingestion(mapped_results):
        # Airflow supplies mapped XCom values as a lazy sequence.
        mapped_results = list(mapped_results)
        summary = {
            "discovered": len(mapped_results),
            "ingested": [],
            "already_ingested": [],
            "unsupported": [],
            "failed": [],
        }

        for item in mapped_results:
            status = item.get("status")
            if status == "INGESTED":
                summary["ingested"].append(item)
            elif status == "ALREADY_INGESTED":
                summary["already_ingested"].append(item)
            elif status == "UNSUPPORTED":
                summary["unsupported"].append(item["filename"])
            elif status == "FAILED":
                summary["failed"].append(item)

        print(
            "Parallel Bronze ingestion summary: "
            f"discovered={summary['discovered']}, "
            f"ingested={len(summary['ingested'])}, "
            f"already_ingested={len(summary['already_ingested'])}, "
            f"unsupported={len(summary['unsupported'])}, "
            f"failed={len(summary['failed'])}"
        )
        return summary

    # --------------------------------------------------
    # 3. IDENTIFY BRONZE FILES PENDING SILVER PROCESSING
    # --------------------------------------------------

    @task
    def identify_pending_silver_files(ingestion_summary):
        hook = SnowflakeHook(
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )
        conn = hook.get_conn()
        cursor = conn.cursor()

        pending_sources = []

        try:
            cursor.execute(f"USE WAREHOUSE {WAREHOUSE}")

            for config in SOURCE_CONFIG:
                pending_file_names = count_pending_files(cursor, config)

                if pending_file_names:
                    pending_sources.append({
                        "source_table": config["table"],
                        "target_table": config["silver_table"],
                        "sql_file": config["silver_sql_file"],
                        "source_file_names": pending_file_names,
                    })

                    print(
                        f"{config['table']} has "
                        f"{len(pending_file_names)} pending Bronze file(s): "
                        f"{pending_file_names}"
                    )

            if not pending_sources:
                print("No Bronze files are pending Silver processing.")

        finally:
            cursor.close()
            conn.close()

        return pending_sources

    # --------------------------------------------------
    # 4. EXECUTE CORRESPONDING SILVER SQL FILES
    # --------------------------------------------------

    @task
    def execute_silver_sql_files(pending_sources):
        if not pending_sources:
            print("No Silver SQL files need to run.")
            return {"processed": [], "failed": []}

        sql_directory = Path(SQL_DIR)
        if not sql_directory.is_dir():
            raise FileNotFoundError(
                f"Silver SQL directory is not mounted: {SQL_DIR}"
            )

        context = get_current_context()
        airflow_run_id = context["run_id"][:250]

        hook = SnowflakeHook(
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )
        conn = hook.get_conn()
        audit_cursor = conn.cursor()

        result = {"processed": [], "failed": []}

        try:
            audit_cursor.execute(f"USE WAREHOUSE {WAREHOUSE}")

            for source in pending_sources:
                sql_file = sql_directory / source["sql_file"]
                config = next(
                    item for item in SOURCE_CONFIG
                    if item["table"] == source["source_table"]
                )
                pending_file_names = source["source_file_names"]

                try:
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
                        f"Executing {sql_file.name} for "
                        f"{source['source_table']} -> {source['target_table']}"
                    )
                    print(
                        f"Pending source files: {pending_file_names}"
                    )

                    statement_count = 0

                    # execute_stream runs statements on this same Snowflake
                    # connection/session. This is required because the SQL
                    # scripts create and reuse session temporary tables.
                    for statement_cursor in conn.execute_stream(
                        StringIO(sql_text),
                        remove_comments=True,
                    ):
                        statement_count += 1
                        try:
                            if statement_cursor.description:
                                rows = statement_cursor.fetchall()
                                if rows:
                                    print(
                                        f"{sql_file.name}, statement "
                                        f"{statement_count}: "
                                        f"{len(rows)} result row(s)"
                                    )
                            else:
                                print(
                                    f"{sql_file.name}, statement "
                                    f"{statement_count} completed."
                                )
                        finally:
                            statement_cursor.close()

                    # Attach the actual Silver Airflow run to the latest
                    # successful audit records for the pending files.
                    placeholders = ", ".join(
                        ["%s"] * len(pending_file_names)
                    )
                    audit_cursor.execute(
                        f"""
                        UPDATE {SILVER_LOAD_CONTROL}
                        SET AIRFLOW_RUN_ID = %s
                        WHERE SOURCE_TABLE = %s
                          AND TARGET_TABLE = %s
                          AND LOAD_STATUS = 'SUCCESS'
                          AND SRC_FILE_NM IN ({placeholders})
                        """,
                        tuple(
                            [airflow_run_id, source["source_table"],
                             source["target_table"]]
                            + pending_file_names
                        ),
                    )

                    result["processed"].append({
                        "source_table": source["source_table"],
                        "target_table": source["target_table"],
                        "sql_file": source["sql_file"],
                        "source_file_names": pending_file_names,
                    })

                    print(
                        f"Completed SQL execution for "
                        f"{source['source_table']}."
                    )

                except Exception as sql_error:
                    error_message = str(sql_error)[:4000]
                    print(
                        f"Silver SQL failed for {source['source_table']}: "
                        f"{error_message}"
                    )

                    # Persist failure for every file that caused this
                    # source-table SQL to be selected. These records remain
                    # pending and are retried on a later DAG run.
                    record_silver_failure(
                        audit_cursor,
                        config,
                        pending_file_names,
                        airflow_run_id,
                        error_message,
                    )

                    result["failed"].append({
                        "source_table": source["source_table"],
                        "sql_file": source["sql_file"],
                        "source_file_names": pending_file_names,
                        "error": error_message,
                    })

        finally:
            audit_cursor.close()
            conn.close()

        return result

    # --------------------------------------------------
    # 5. CONFIRM SILVER PROCESSING AND AUDIT RESULTS
    # --------------------------------------------------

    @task
    def confirm_silver_processing(
        pending_sources,
        ingestion_summary,
        silver_execution_summary,
    ):
        hook = SnowflakeHook(
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )
        conn = hook.get_conn()
        cursor = conn.cursor()

        audit_errors = []
        audited_files = 0

        try:
            cursor.execute(f"USE WAREHOUSE {WAREHOUSE}")

            for source in pending_sources:
                source_table = source["source_table"]
                target_table = source["target_table"]
                source_file_names = source["source_file_names"]

                if not source_file_names:
                    continue

                config = next(
                    item for item in SOURCE_CONFIG
                    if item["table"] == source_table
                )
                bronze_table = (
                    f"{DATABASE}.{BRONZE_SCHEMA}.{source_table}"
                )

                placeholders = ", ".join(
                    ["%s"] * len(source_file_names)
                )

                audit_query = f"""
                    WITH BRONZE_COUNTS AS (
                        SELECT
                            SRC_FILE_NM,
                            COUNT(DISTINCT ROW_NUM) AS BRONZE_ROWS
                        FROM {bronze_table}
                        WHERE SRC_FILE_NM IN ({placeholders})
                        GROUP BY SRC_FILE_NM
                    ),
                    CONTROL_RANKED AS (
                        SELECT
                            SRC_FILE_NM,
                            LOAD_STATUS,
                            ROWS_READ,
                            ROWS_LOADED,
                            ROWS_QUARANTINED,
                            COMPLETED_TS,
                            ERROR_MESSAGE,
                            ROW_NUMBER() OVER (
                                PARTITION BY SRC_FILE_NM
                                ORDER BY
                                    COALESCE(COMPLETED_TS, STARTED_TS) DESC NULLS LAST,
                                    STARTED_TS DESC NULLS LAST,
                                    CONTROL_ID DESC
                            ) AS RN
                        FROM {SILVER_LOAD_CONTROL}
                        WHERE SOURCE_TABLE = %s
                          AND TARGET_TABLE = %s
                          AND SRC_FILE_NM IN ({placeholders})
                    )
                    SELECT
                        B.SRC_FILE_NM,
                        B.BRONZE_ROWS,
                        C.LOAD_STATUS,
                        C.ROWS_READ,
                        C.ROWS_LOADED,
                        C.ROWS_QUARANTINED,
                        C.COMPLETED_TS,
                        C.ERROR_MESSAGE
                    FROM BRONZE_COUNTS B
                    LEFT JOIN CONTROL_RANKED C
                        ON C.SRC_FILE_NM = B.SRC_FILE_NM
                       AND C.RN = 1
                    ORDER BY B.SRC_FILE_NM
                """

                params = (
                    tuple(source_file_names)
                    + (source_table, target_table)
                    + tuple(source_file_names)
                )
                cursor.execute(audit_query, params)
                audit_rows = cursor.fetchall()

                rows_by_file = {
                    row[0]: row for row in audit_rows
                }

                for source_file_name in source_file_names:
                    audited_files += 1
                    row = rows_by_file.get(source_file_name)

                    if row is None:
                        audit_errors.append(
                            f"{source_table}/{source_file_name}: "
                            "no Bronze records found for pending source file."
                        )
                        continue

                    (
                        _,
                        bronze_rows,
                        load_status,
                        rows_read,
                        rows_loaded,
                        rows_quarantined,
                        completed_ts,
                        error_message,
                    ) = row

                    problems = []

                    if load_status != "SUCCESS":
                        problems.append(
                            f"latest LOAD_STATUS is {load_status!r}, not SUCCESS"
                        )

                    if completed_ts is None:
                        problems.append("COMPLETED_TS is NULL")

                    if error_message:
                        problems.append(f"ERROR_MESSAGE={error_message}")

                    if rows_read is None or int(rows_read) != int(bronze_rows):
                        problems.append(
                            f"ROWS_READ={rows_read} does not match "
                            f"Bronze lineage row count={bronze_rows}"
                        )

                    loaded = int(rows_loaded or 0)
                    quarantined = int(rows_quarantined or 0)
                    read_count = int(rows_read or 0)

                    if loaded < 0 or quarantined < 0:
                        problems.append("negative audit row counts")

                    if loaded + quarantined > read_count:
                        problems.append(
                            "ROWS_LOADED + ROWS_QUARANTINED exceeds ROWS_READ"
                        )

                    if problems:
                        audit_errors.append(
                            f"{source_table}/{source_file_name}: "
                            + "; ".join(problems)
                        )
                    else:
                        print(
                            f"AUDIT OK | {source_table} | "
                            f"{source_file_name} | read={rows_read}, "
                            f"loaded={rows_loaded}, "
                            f"quarantined={rows_quarantined}"
                        )

        finally:
            cursor.close()
            conn.close()

        ingestion_failures = ingestion_summary.get("failed", [])
        silver_failures = silver_execution_summary.get("failed", [])

        print(
            f"Silver audit completed for {audited_files} pending source file(s)."
        )

        all_errors = []

        if ingestion_failures:
            all_errors.append(
                "Bronze ingestion failures: "
                + "; ".join(
                    f"{item['filename']}: {item['error']}"
                    for item in ingestion_failures
                )
            )

        if silver_failures:
            all_errors.append(
                "Silver SQL failures: "
                + "; ".join(
                    f"{item['source_table']}: {item['error']}"
                    for item in silver_failures
                )
            )

        if audit_errors:
            all_errors.append(
                "Silver audit failures: " + "; ".join(audit_errors)
            )

        if all_errors:
            raise RuntimeError(
                "Bronze-to-Silver pipeline did not complete cleanly:\n"
                + "\n".join(all_errors)
            )

        print("Bronze-to-Silver pipeline completed successfully.")

    # --------------------------------------------------
    # TASK DEPENDENCIES: 1 -> 2 -> 3 -> 4 -> 5
    # --------------------------------------------------

    connection_test = test_snowflake_connection()
    file_sensor = wait_for_required_files()
    ingestion_log_ready = prepare_ingestion_log()
    files_to_map = discover_files_for_mapping(file_sensor)
    mapped_ingestion = ingest_single_file.expand(file_info=files_to_map)
    bronze_ingestion = summarize_ingestion(mapped_ingestion)
    pending_files = identify_pending_silver_files(bronze_ingestion)
    silver_execution = execute_silver_sql_files(pending_files)
    silver_audit = confirm_silver_processing(
        pending_files,
        bronze_ingestion,
        silver_execution,
    )

    connection_test >> ingestion_log_ready >> file_sensor
    file_sensor >> files_to_map >> mapped_ingestion
    mapped_ingestion >> bronze_ingestion
    bronze_ingestion >> pending_files >> silver_execution >> silver_audit


retail_stg_ingestion()
