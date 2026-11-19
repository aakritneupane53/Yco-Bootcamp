-- ============================================================
-- RETAIL MEDALLION PROJECT
-- Bronze -> Silver: STORE MASTER
-- Source : RETAIL_DB.STG.STG_STORE_MASTER
-- Targets: RETAIL_DB.TMP.SILVER_STORE_MASTER
--          RETAIL_DB.TMP.REJ_STORE_MASTER
--          RETAIL_DB.TMP.SILVER_LOAD_CONTROL
--
-- Grain: one store per source snapshot row.
-- Idempotency key: SRC_FILE_NM + ROW_NUM.
--
-- Required business keys assumed for a store master record:
-- STORE_ID, REGION_ID, COUNTRY_ID.
-- ACTIVE_FLG must be convertible to BOOLEAN.
-- SNAPSHOT_DT is derived from YYYYMMDD in SRC_FILE_NM.
--
-- Duplicate policy:
--   * Same normalized STORE_ID + same normalized payload:
--       keep the first source row; ignore later exact duplicates.
--   * Same normalized STORE_ID + different normalized payload:
--       quarantine ALL rows for that STORE_ID in that source file.
--
-- This script preserves each source snapshot. It does not overwrite
-- a store's earlier snapshot with a later snapshot.
-- ============================================================

USE DATABASE RETAIL_DB;
USE SCHEMA TMP;

-- ------------------------------------------------------------
-- STEP 1: Build a typed work table from Bronze.
-- Keep raw source values alongside normalized/cast values so the
-- rejection table retains the original source representation.
-- ------------------------------------------------------------

CREATE OR REPLACE TEMP TABLE TMP_STORE_MASTER_WORK AS
WITH BRONZE_DEDUP AS (
    -- Guard against the same file/row being present more than once
    -- in Bronze due to a replay. Keep the latest ingested copy.
    SELECT
        STORE_ID,
        STORE_NAME,
        REGION_ID,
        REGION_NAME,
        COUNTRY_ID,
        COUNTRY_NAME,
        CITY,
        STORE_TYPE,
        ACTIVE_FLG,
        SRC_FILE_NM,
        ROW_NUM,
        INGEST_TS AS SOURCE_INGEST_TS,
        BATCH_ID
    FROM RETAIL_DB.STG.STG_STORE_MASTER
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SRC_FILE_NM, ROW_NUM
        ORDER BY INGEST_TS DESC, BATCH_ID DESC
    ) = 1
),
NORMALIZED AS (
    SELECT
        -- Original values for rejection/audit.
        STORE_ID       AS RAW_STORE_ID,
        STORE_NAME     AS RAW_STORE_NAME,
        REGION_ID      AS RAW_REGION_ID,
        REGION_NAME    AS RAW_REGION_NAME,
        COUNTRY_ID     AS RAW_COUNTRY_ID,
        COUNTRY_NAME   AS RAW_COUNTRY_NAME,
        CITY           AS RAW_CITY,
        STORE_TYPE     AS RAW_STORE_TYPE,
        ACTIVE_FLG     AS RAW_ACTIVE_FLG,

        -- Normalized values for Silver.
        NULLIF(UPPER(TRIM(STORE_ID)), '')       AS STORE_ID,
        NULLIF(TRIM(STORE_NAME), '')            AS STORE_NAME,
        NULLIF(UPPER(TRIM(REGION_ID)), '')      AS REGION_ID,
        NULLIF(TRIM(REGION_NAME), '')           AS REGION_NAME,
        NULLIF(UPPER(TRIM(COUNTRY_ID)), '')     AS COUNTRY_ID,
        NULLIF(TRIM(COUNTRY_NAME), '')          AS COUNTRY_NAME,
        NULLIF(TRIM(CITY), '')                  AS CITY,
        NULLIF(UPPER(TRIM(STORE_TYPE)), '')     AS STORE_TYPE,

        CASE
            WHEN UPPER(TRIM(ACTIVE_FLG)) IN ('1', 'TRUE', 'T', 'Y', 'YES')
                THEN TRUE
            WHEN UPPER(TRIM(ACTIVE_FLG)) IN ('0', 'FALSE', 'F', 'N', 'NO')
                THEN FALSE
            ELSE NULL
        END AS ACTIVE_FLG,

        TRY_TO_DATE(
            REGEXP_SUBSTR(SRC_FILE_NM, '[0-9]{8}'),
            'YYYYMMDD'
        ) AS SNAPSHOT_DT,

        SRC_FILE_NM,
        ROW_NUM,
        SOURCE_INGEST_TS,
        BATCH_ID
    FROM BRONZE_DEDUP
),
HASHED AS (
    SELECT
        N.*,
        -- Hash of raw values. Useful for identifying source changes.
        SHA2(
            TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(
                'STORE_ID', RAW_STORE_ID,
                'STORE_NAME', RAW_STORE_NAME,
                'REGION_ID', RAW_REGION_ID,
                'REGION_NAME', RAW_REGION_NAME,
                'COUNTRY_ID', RAW_COUNTRY_ID,
                'COUNTRY_NAME', RAW_COUNTRY_NAME,
                'CITY', RAW_CITY,
                'STORE_TYPE', RAW_STORE_TYPE,
                'ACTIVE_FLG', RAW_ACTIVE_FLG
            )),
            256
        ) AS SRC_ROW_HASH,
        -- Hash of normalized values. Used to distinguish exact
        -- duplicates from conflicting records with the same key.
        SHA2(
            TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(
                'STORE_ID', STORE_ID,
                'STORE_NAME', STORE_NAME,
                'REGION_ID', REGION_ID,
                'REGION_NAME', REGION_NAME,
                'COUNTRY_ID', COUNTRY_ID,
                'COUNTRY_NAME', COUNTRY_NAME,
                'CITY', CITY,
                'STORE_TYPE', STORE_TYPE,
                'ACTIVE_FLG', ACTIVE_FLG,
                'SNAPSHOT_DT', SNAPSHOT_DT
            )),
            256
        ) AS NORMALIZED_ROW_HASH
    FROM NORMALIZED N
),
DUPLICATE_PROFILE AS (
    SELECT
        SRC_FILE_NM,
        STORE_ID,
        COUNT(*) AS STORE_ID_ROW_COUNT,
        COUNT(DISTINCT NORMALIZED_ROW_HASH) AS DISTINCT_PAYLOAD_COUNT
    FROM HASHED
    WHERE STORE_ID IS NOT NULL
    GROUP BY SRC_FILE_NM, STORE_ID
),
RANKED AS (
    SELECT
        H.*,
        P.STORE_ID_ROW_COUNT,
        P.DISTINCT_PAYLOAD_COUNT,
        ROW_NUMBER() OVER (
            PARTITION BY H.SRC_FILE_NM, H.STORE_ID, H.NORMALIZED_ROW_HASH
            ORDER BY H.ROW_NUM
        ) AS EXACT_DUPLICATE_RN
    FROM HASHED H
    LEFT JOIN DUPLICATE_PROFILE P
        ON H.SRC_FILE_NM = P.SRC_FILE_NM
       AND H.STORE_ID = P.STORE_ID
)
SELECT
    R.*,
    COALESCE(
        STORE_ID_ROW_COUNT > 1 AND DISTINCT_PAYLOAD_COUNT > 1,
        FALSE
    ) AS CONFLICTING_DUPLICATE_FLG,
    NULLIF(
        ARRAY_TO_STRING(
            ARRAY_CONSTRUCT_COMPACT(
                IFF(STORE_ID IS NULL, 'MISSING_STORE_ID', NULL),
                IFF(REGION_ID IS NULL, 'MISSING_REGION_ID', NULL),
                IFF(COUNTRY_ID IS NULL, 'MISSING_COUNTRY_ID', NULL),
                IFF(SNAPSHOT_DT IS NULL, 'INVALID_SNAPSHOT_DATE_IN_FILENAME', NULL),
                IFF(ACTIVE_FLG IS NULL, 'INVALID_ACTIVE_FLG', NULL),
                IFF(
                    COALESCE(STORE_ID_ROW_COUNT > 1 AND DISTINCT_PAYLOAD_COUNT > 1, FALSE),
                    'CONFLICTING_DUPLICATE_STORE_ID_IN_FILE',
                    NULL
                )
            ),
            '; '
        ),
        ''
    ) AS REJECT_REASON,
    CASE
        WHEN STORE_ID IS NULL THEN 'MISSING_STORE_ID'
        WHEN REGION_ID IS NULL THEN 'MISSING_REGION_ID'
        WHEN COUNTRY_ID IS NULL THEN 'MISSING_COUNTRY_ID'
        WHEN SNAPSHOT_DT IS NULL THEN 'INVALID_SNAPSHOT_DATE'
        WHEN ACTIVE_FLG IS NULL THEN 'INVALID_ACTIVE_FLG'
        WHEN COALESCE(STORE_ID_ROW_COUNT > 1 AND DISTINCT_PAYLOAD_COUNT > 1, FALSE)
            THEN 'CONFLICTING_DUPLICATE_STORE_ID'
        ELSE NULL
    END AS REJECT_CODE
FROM RANKED R;

-- ------------------------------------------------------------
-- STEP 2: Upsert invalid source rows into REJ_STORE_MASTER.
-- One rejection record per source file + row + quality gate.
-- Raw Bronze values are retained as VARCHAR.
-- ------------------------------------------------------------

MERGE INTO RETAIL_DB.TMP.REJ_STORE_MASTER T
USING (
    SELECT *
    FROM TMP_STORE_MASTER_WORK
    WHERE REJECT_REASON IS NOT NULL
) S
    ON T.SRC_FILE_NM = S.SRC_FILE_NM
   AND T.ROW_NUM = S.ROW_NUM
   AND T.QUALITY_GATE = 'GATE_1_BRONZE_TO_SILVER'
WHEN MATCHED THEN UPDATE SET
    T.STORE_ID = S.RAW_STORE_ID,
    T.STORE_NAME = S.RAW_STORE_NAME,
    T.REGION_ID = S.RAW_REGION_ID,
    T.REGION_NAME = S.RAW_REGION_NAME,
    T.COUNTRY_ID = S.RAW_COUNTRY_ID,
    T.COUNTRY_NAME = S.RAW_COUNTRY_NAME,
    T.CITY = S.RAW_CITY,
    T.STORE_TYPE = S.RAW_STORE_TYPE,
    T.ACTIVE_FLG = S.RAW_ACTIVE_FLG,
    T.SOURCE_INGEST_TS = S.SOURCE_INGEST_TS,
    T.BATCH_ID = S.BATCH_ID,
    T.SRC_ROW_HASH = S.SRC_ROW_HASH,
    T.REJECT_CODE = S.REJECT_CODE,
    T.REJECT_REASON = S.REJECT_REASON,
    T.AIRFLOW_RUN_ID = S.BATCH_ID,
    T.REJECTED_TS = CURRENT_TIMESTAMP(),
    T.RESOLVED_FLG = FALSE,
    T.RESOLVED_TS = NULL
WHEN NOT MATCHED THEN INSERT (
    STORE_ID,
    STORE_NAME,
    REGION_ID,
    REGION_NAME,
    COUNTRY_ID,
    COUNTRY_NAME,
    CITY,
    STORE_TYPE,
    ACTIVE_FLG,
    SRC_FILE_NM,
    ROW_NUM,
    SOURCE_INGEST_TS,
    BATCH_ID,
    SRC_ROW_HASH,
    QUALITY_GATE,
    REJECT_CODE,
    REJECT_REASON,
    REJECTION_ID,
    AIRFLOW_RUN_ID,
    REJECTED_TS,
    RESOLVED_FLG
)
VALUES (
    S.RAW_STORE_ID,
    S.RAW_STORE_NAME,
    S.RAW_REGION_ID,
    S.RAW_REGION_NAME,
    S.RAW_COUNTRY_ID,
    S.RAW_COUNTRY_NAME,
    S.RAW_CITY,
    S.RAW_STORE_TYPE,
    S.RAW_ACTIVE_FLG,
    S.SRC_FILE_NM,
    S.ROW_NUM,
    S.SOURCE_INGEST_TS,
    S.BATCH_ID,
    S.SRC_ROW_HASH,
    'GATE_1_BRONZE_TO_SILVER',
    S.REJECT_CODE,
    S.REJECT_REASON,
    UUID_STRING(),
    S.BATCH_ID,
    CURRENT_TIMESTAMP(),
    FALSE
);

-- ------------------------------------------------------------
-- STEP 3: Mark previously rejected rows as resolved if they now
-- pass Gate 1. Keep the rejection history for audit purposes.
-- ------------------------------------------------------------

UPDATE RETAIL_DB.TMP.REJ_STORE_MASTER T
SET
    RESOLVED_FLG = TRUE,
    RESOLVED_TS = COALESCE(T.RESOLVED_TS, CURRENT_TIMESTAMP())
FROM TMP_STORE_MASTER_WORK S
WHERE T.SRC_FILE_NM = S.SRC_FILE_NM
  AND T.ROW_NUM = S.ROW_NUM
  AND T.QUALITY_GATE = 'GATE_1_BRONZE_TO_SILVER'
  AND S.REJECT_REASON IS NULL;

-- ------------------------------------------------------------
-- STEP 4: Remove Silver rows that are now invalid or are later
-- copies of an exact duplicate payload from the same source file.
-- ------------------------------------------------------------

DELETE FROM RETAIL_DB.TMP.SILVER_STORE_MASTER T
USING TMP_STORE_MASTER_WORK S
WHERE T.SRC_FILE_NM = S.SRC_FILE_NM
  AND T.ROW_NUM = S.ROW_NUM
  AND (
      S.REJECT_REASON IS NOT NULL
      OR S.EXACT_DUPLICATE_RN > 1
  );

-- ------------------------------------------------------------
-- STEP 5: Merge valid, non-duplicate rows into Silver.
-- Source lineage, not STORE_ID alone, is the idempotency key.
-- ------------------------------------------------------------

MERGE INTO RETAIL_DB.TMP.SILVER_STORE_MASTER T
USING (
    SELECT *
    FROM TMP_STORE_MASTER_WORK
    WHERE REJECT_REASON IS NULL
      AND EXACT_DUPLICATE_RN = 1
) S
    ON T.SRC_FILE_NM = S.SRC_FILE_NM
   AND T.ROW_NUM = S.ROW_NUM
WHEN MATCHED AND COALESCE(T.SRC_ROW_HASH, '') <> S.SRC_ROW_HASH THEN UPDATE SET
    T.STORE_ID = S.STORE_ID,
    T.STORE_NAME = S.STORE_NAME,
    T.REGION_ID = S.REGION_ID,
    T.REGION_NAME = S.REGION_NAME,
    T.COUNTRY_ID = S.COUNTRY_ID,
    T.COUNTRY_NAME = S.COUNTRY_NAME,
    T.CITY = S.CITY,
    T.STORE_TYPE = S.STORE_TYPE,
    T.ACTIVE_FLG = S.ACTIVE_FLG,
    T.SNAPSHOT_DT = S.SNAPSHOT_DT,
    T.SRC_ROW_HASH = S.SRC_ROW_HASH,
    T.BATCH_ID = S.BATCH_ID,
    T.UPDATE_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    STORE_ID,
    STORE_NAME,
    REGION_ID,
    REGION_NAME,
    COUNTRY_ID,
    COUNTRY_NAME,
    CITY,
    STORE_TYPE,
    ACTIVE_FLG,
    SNAPSHOT_DT,
    SRC_FILE_NM,
    ROW_NUM,
    SRC_ROW_HASH,
    SRC_SYS_C,
    BATCH_ID,
    INSERT_TS,
    UPDATE_TS
)
VALUES (
    S.STORE_ID,
    S.STORE_NAME,
    S.REGION_ID,
    S.REGION_NAME,
    S.COUNTRY_ID,
    S.COUNTRY_NAME,
    S.CITY,
    S.STORE_TYPE,
    S.ACTIVE_FLG,
    S.SNAPSHOT_DT,
    S.SRC_FILE_NM,
    S.ROW_NUM,
    S.SRC_ROW_HASH,
    'RETAIL_CSV',
    S.BATCH_ID,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);

-- ------------------------------------------------------------
-- STEP 6: Upsert file-level processing results into control.
-- The control row records the latest result per source file/target.
-- Row-level MERGE above is the idempotency mechanism.
--
-- SRC_FILE_SIZE_BYTES and SRC_FILE_CHECKSUM remain NULL because
-- those values are not currently stored in STG_STORE_MASTER.
-- AIRFLOW_RUN_ID temporarily uses the Bronze BATCH_ID (which is
-- the Bronze DAG run ID), matching the current Item Master script.
-- When Silver is orchestrated, this can be changed to the Silver DAG run ID.
-- ------------------------------------------------------------

MERGE INTO RETAIL_DB.TMP.SILVER_LOAD_CONTROL T
USING (
    SELECT
        'STG_STORE_MASTER' AS SOURCE_TABLE,
        'SILVER_STORE_MASTER' AS TARGET_TABLE,
        SRC_FILE_NM,
        MAX(BATCH_ID) AS BATCH_ID,
        COUNT(*) AS ROWS_READ,
        COUNT_IF(REJECT_REASON IS NULL AND EXACT_DUPLICATE_RN = 1) AS ROWS_LOADED,
        COUNT_IF(REJECT_REASON IS NOT NULL) AS ROWS_QUARANTINED
    FROM TMP_STORE_MASTER_WORK
    GROUP BY SRC_FILE_NM
) S
    ON T.SOURCE_TABLE = S.SOURCE_TABLE
   AND T.TARGET_TABLE = S.TARGET_TABLE
   AND T.SRC_FILE_NM = S.SRC_FILE_NM
WHEN MATCHED THEN UPDATE SET
    T.BATCH_ID = S.BATCH_ID,
    T.AIRFLOW_RUN_ID = S.BATCH_ID,
    T.LOAD_STATUS = 'SUCCESS',
    T.STARTED_TS = CURRENT_TIMESTAMP(),
    T.COMPLETED_TS = CURRENT_TIMESTAMP(),
    T.ROWS_READ = S.ROWS_READ,
    T.ROWS_LOADED = S.ROWS_LOADED,
    T.ROWS_QUARANTINED = S.ROWS_QUARANTINED,
    T.ERROR_MESSAGE = NULL
WHEN NOT MATCHED THEN INSERT (
    CONTROL_ID,
    SOURCE_TABLE,
    TARGET_TABLE,
    SRC_FILE_NM,
    SRC_FILE_SIZE_BYTES,
    SRC_FILE_CHECKSUM,
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
VALUES (
    UUID_STRING(),
    S.SOURCE_TABLE,
    S.TARGET_TABLE,
    S.SRC_FILE_NM,
    NULL,
    NULL,
    S.BATCH_ID,
    S.BATCH_ID,
    'SUCCESS',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    S.ROWS_READ,
    S.ROWS_LOADED,
    S.ROWS_QUARANTINED,
    NULL
);

-- ------------------------------------------------------------
-- STEP 7: Validation queries.
-- For the sample currently shared, expected results are:
--   SILVER_STORE_MASTER: 2 rows (S001, S002)
--   REJ_STORE_MASTER:    2 Gate-1 rejected rows (both S003 rows)
-- ------------------------------------------------------------

SELECT
    SRC_FILE_NM,
    SNAPSHOT_DT,
    COUNT(*) AS ROW_COUNT,
    COUNT(DISTINCT STORE_ID) AS DISTINCT_STORE_COUNT
FROM RETAIL_DB.TMP.SILVER_STORE_MASTER
GROUP BY SRC_FILE_NM, SNAPSHOT_DT
ORDER BY SNAPSHOT_DT;

SELECT
    SRC_FILE_NM,
    ROW_NUM,
    STORE_ID,
    STORE_TYPE,
    QUALITY_GATE,
    REJECT_CODE,
    REJECT_REASON,
    RESOLVED_FLG
FROM RETAIL_DB.TMP.REJ_STORE_MASTER
WHERE QUALITY_GATE = 'GATE_1_BRONZE_TO_SILVER'
ORDER BY SRC_FILE_NM, ROW_NUM;

SELECT
    SOURCE_TABLE,
    TARGET_TABLE,
    SRC_FILE_NM,
    LOAD_STATUS,
    ROWS_READ,
    ROWS_LOADED,
    ROWS_QUARANTINED,
    AIRFLOW_RUN_ID
FROM RETAIL_DB.TMP.SILVER_LOAD_CONTROL
WHERE SOURCE_TABLE = 'STG_STORE_MASTER'
  AND TARGET_TABLE = 'SILVER_STORE_MASTER'
ORDER BY SRC_FILE_NM;

-- End of Store Master Bronze -> Silver transformation.
