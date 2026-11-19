USE DATABASE RETAIL_DB;
USE SCHEMA TMP;
USE WAREHOUSE RETAIL_WH;

-- ===========================================================================
-- CUSTOMER MASTER: BRONZE -> SILVER
--
-- Business key: CUSTOMER_ID
-- Silver grain: one current row per CUSTOMER_ID.
--
-- Duplicate policy:
--   1. Missing IDs and invalid values are quarantined.
--   2. Duplicate IDs within the same source file are checked using the
--      normalized customer payload. Conflicting duplicates are all rejected;
--      exact duplicates retain the first source row and reject later rows.
--   3. The same CUSTOMER_ID appearing in different source files is treated as
--      a later version of the same customer, not as a within-file duplicate.
--      The latest ingested valid version is selected for the Silver MERGE.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 1. Standardize, type-cast, hash, and classify Bronze rows.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE TMP_CUSTOMER_MASTER_CLASSIFIED AS
WITH raw_source AS (
    SELECT
        CUSTOMER_ID AS CUSTOMER_ID_RAW,
        CUSTOMER_NAME AS CUSTOMER_NAME_RAW,
        CITY AS CITY_RAW,
        LOYALTY_TIER AS LOYALTY_TIER_RAW,
        REGISTRATION_DATE AS REGISTRATION_DATE_RAW,
        ACTIVE_FLG AS ACTIVE_FLG_RAW,
        SRC_FILE_NM,
        ROW_NUM,
        INGEST_TS,
        BATCH_ID
    FROM RETAIL_DB.STG.STG_CUSTOMER_MASTER
),
normalized AS (
    SELECT
        NULLIF(UPPER(TRIM(CUSTOMER_ID_RAW)), '') AS CUSTOMER_ID,
        NULLIF(TRIM(CUSTOMER_NAME_RAW), '') AS CUSTOMER_NAME,
        NULLIF(TRIM(CITY_RAW), '') AS CITY,
        NULLIF(UPPER(TRIM(LOYALTY_TIER_RAW)), '') AS LOYALTY_TIER,
        NULLIF(TRIM(REGISTRATION_DATE_RAW), '') AS REGISTRATION_DATE_RAW,
        CASE
            WHEN UPPER(TRIM(ACTIVE_FLG_RAW)) IN ('1', 'TRUE', 'Y') THEN TRUE
            WHEN UPPER(TRIM(ACTIVE_FLG_RAW)) IN ('0', 'FALSE', 'N') THEN FALSE
            ELSE NULL
        END AS ACTIVE_FLG,
        NULLIF(TRIM(ACTIVE_FLG_RAW), '') AS ACTIVE_FLG_RAW,
        SRC_FILE_NM,
        ROW_NUM,
        INGEST_TS,
        BATCH_ID,
        SHA2(
            TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(
                'CUSTOMER_ID', CUSTOMER_ID_RAW,
                'CUSTOMER_NAME', CUSTOMER_NAME_RAW,
                'CITY', CITY_RAW,
                'LOYALTY_TIER', LOYALTY_TIER_RAW,
                'REGISTRATION_DATE', REGISTRATION_DATE_RAW,
                'ACTIVE_FLG', ACTIVE_FLG_RAW
            )),
            256
        ) AS SRC_ROW_HASH
    FROM raw_source
),
typed AS (
    SELECT
        *,
        TRY_TO_DATE(REGISTRATION_DATE_RAW, 'YYYY-MM-DD') AS REGISTRATION_DATE
    FROM normalized
),
hashed AS (
    SELECT
        *,
        SHA2(
            TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(
                'CUSTOMER_ID', CUSTOMER_ID,
                'CUSTOMER_NAME', CUSTOMER_NAME,
                'CITY', CITY,
                'LOYALTY_TIER', LOYALTY_TIER,
                'REGISTRATION_DATE', REGISTRATION_DATE,
                'ACTIVE_FLG', ACTIVE_FLG
            )),
            256
        ) AS NORMALIZED_PAYLOAD_HASH
    FROM typed
),
duplicate_stats AS (
    -- Deliberately scope this check to one source file.
    -- Cross-file versions are resolved later when preparing the Silver MERGE.
    SELECT
        SRC_FILE_NM,
        CUSTOMER_ID,
        COUNT(*) AS KEY_ROW_COUNT,
        COUNT(DISTINCT NORMALIZED_PAYLOAD_HASH) AS DISTINCT_PAYLOAD_COUNT
    FROM hashed
    WHERE CUSTOMER_ID IS NOT NULL
    GROUP BY SRC_FILE_NM, CUSTOMER_ID
),
ranked AS (
    SELECT
        H.*,
        COALESCE(D.KEY_ROW_COUNT, 1) AS KEY_ROW_COUNT,
        COALESCE(D.DISTINCT_PAYLOAD_COUNT, 1) AS DISTINCT_PAYLOAD_COUNT,
        ROW_NUMBER() OVER (
            PARTITION BY H.SRC_FILE_NM, H.CUSTOMER_ID
            ORDER BY
                H.ROW_NUM ASC NULLS LAST,
                H.INGEST_TS DESC NULLS LAST,
                H.BATCH_ID DESC NULLS LAST
        ) AS DUPLICATE_RN
    FROM hashed H
    LEFT JOIN duplicate_stats D
        ON H.SRC_FILE_NM = D.SRC_FILE_NM
       AND H.CUSTOMER_ID = D.CUSTOMER_ID
)
SELECT
    *,
    CASE
        WHEN CUSTOMER_ID IS NULL
            THEN 'MISSING_CUSTOMER_ID'
        WHEN REGISTRATION_DATE_RAW IS NOT NULL
             AND REGISTRATION_DATE IS NULL
            THEN 'INVALID_REGISTRATION_DATE'
        WHEN ACTIVE_FLG_RAW IS NOT NULL
             AND ACTIVE_FLG IS NULL
            THEN 'INVALID_ACTIVE_FLAG'
        WHEN KEY_ROW_COUNT > 1 AND DISTINCT_PAYLOAD_COUNT > 1
            THEN 'CONFLICTING_DUPLICATE_CUSTOMER_ID'
        WHEN KEY_ROW_COUNT > 1 AND DUPLICATE_RN > 1
            THEN 'DUPLICATE_SOURCE_RECORD'
        ELSE NULL
    END AS REJECT_CODE,
    CASE
        WHEN CUSTOMER_ID IS NULL
            THEN 'CUSTOMER_ID is NULL, empty, or whitespace.'
        WHEN REGISTRATION_DATE_RAW IS NOT NULL
             AND REGISTRATION_DATE IS NULL
            THEN 'REGISTRATION_DATE could not be converted using YYYY-MM-DD.'
        WHEN ACTIVE_FLG_RAW IS NOT NULL
             AND ACTIVE_FLG IS NULL
            THEN 'ACTIVE_FLG must be one of 1, 0, TRUE, FALSE, Y, or N.'
        WHEN KEY_ROW_COUNT > 1 AND DISTINCT_PAYLOAD_COUNT > 1
            THEN 'CUSTOMER_ID occurs more than once in the same source file with different normalized attributes; all conflicting rows are quarantined.'
        WHEN KEY_ROW_COUNT > 1 AND DUPLICATE_RN > 1
            THEN 'Exact duplicate CUSTOMER_ID record within the same source file; retained the first row.'
        ELSE NULL
    END AS REJECT_REASON
FROM ranked;

-- ---------------------------------------------------------------------------
-- 2. Prepare one unique load-control ID per source file for this execution.
--    Keeping these IDs in a temp table ensures step 6 updates only the audit
--    rows inserted by this execution, not older STARTED rows from prior runs.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE TMP_CUSTOMER_MASTER_CONTROL_ROWS AS
SELECT
    UUID_STRING() AS CONTROL_ID,
    SRC_FILE_NM,
    MAX(BATCH_ID) AS BATCH_ID
FROM TMP_CUSTOMER_MASTER_CLASSIFIED
GROUP BY SRC_FILE_NM;

INSERT INTO RETAIL_DB.TMP.SILVER_LOAD_CONTROL (
    CONTROL_ID,
    SOURCE_TABLE,
    TARGET_TABLE,
    SRC_FILE_NM,
    BATCH_ID,
    AIRFLOW_RUN_ID,
    LOAD_STATUS,
    STARTED_TS
)
SELECT
    CONTROL_ID,
    'STG_CUSTOMER_MASTER',
    'SILVER_CUSTOMER_MASTER',
    SRC_FILE_NM,
    BATCH_ID,
    NULL, -- Populate with the actual Airflow run ID when available.
    'STARTED',
    CURRENT_TIMESTAMP()
FROM TMP_CUSTOMER_MASTER_CONTROL_ROWS;

-- ---------------------------------------------------------------------------
-- 3. Remove stale rejections for source rows that now pass validation.
-- ---------------------------------------------------------------------------
DELETE FROM RETAIL_DB.TMP.REJ_CUSTOMER_MASTER T
USING TMP_CUSTOMER_MASTER_CLASSIFIED S
WHERE T.SRC_FILE_NM = S.SRC_FILE_NM
  AND T.ROW_NUM = S.ROW_NUM
  AND T.QUALITY_GATE = 'GATE_1_BRONZE_TO_SILVER'
  AND S.REJECT_CODE IS NULL;

-- ---------------------------------------------------------------------------
-- 4. Upsert rejected source rows.
--    The source is reduced to one row per rejection lineage key so that a
--    duplicated physical Bronze lineage cannot produce a multi-match MERGE.
-- ---------------------------------------------------------------------------
MERGE INTO RETAIL_DB.TMP.REJ_CUSTOMER_MASTER T
USING (
    SELECT
        CUSTOMER_ID,
        CUSTOMER_NAME,
        CITY,
        LOYALTY_TIER,
        REGISTRATION_DATE_RAW,
        ACTIVE_FLG_RAW,
        SRC_FILE_NM,
        ROW_NUM,
        INGEST_TS,
        BATCH_ID,
        SRC_ROW_HASH,
        REJECT_CODE,
        REJECT_REASON
    FROM TMP_CUSTOMER_MASTER_CLASSIFIED
    WHERE REJECT_CODE IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SRC_FILE_NM, ROW_NUM
        ORDER BY
            INGEST_TS DESC NULLS LAST,
            BATCH_ID DESC NULLS LAST,
            CUSTOMER_ID DESC NULLS LAST
    ) = 1
) S
    ON T.SRC_FILE_NM = S.SRC_FILE_NM
   AND T.ROW_NUM = S.ROW_NUM
   AND T.QUALITY_GATE = 'GATE_1_BRONZE_TO_SILVER'
WHEN MATCHED THEN UPDATE SET
    T.CUSTOMER_ID = S.CUSTOMER_ID,
    T.CUSTOMER_NAME = S.CUSTOMER_NAME,
    T.CITY = S.CITY,
    T.LOYALTY_TIER = S.LOYALTY_TIER,
    T.REGISTRATION_DATE = S.REGISTRATION_DATE_RAW,
    T.ACTIVE_FLG = S.ACTIVE_FLG_RAW,
    T.SOURCE_INGEST_TS = S.INGEST_TS,
    T.BATCH_ID = S.BATCH_ID,
    T.SRC_ROW_HASH = S.SRC_ROW_HASH,
    T.REJECT_CODE = S.REJECT_CODE,
    T.REJECT_REASON = S.REJECT_REASON
WHEN NOT MATCHED THEN INSERT (
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CITY,
    LOYALTY_TIER,
    REGISTRATION_DATE,
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
    AIRFLOW_RUN_ID
) VALUES (
    S.CUSTOMER_ID,
    S.CUSTOMER_NAME,
    S.CITY,
    S.LOYALTY_TIER,
    S.REGISTRATION_DATE_RAW,
    S.ACTIVE_FLG_RAW,
    S.SRC_FILE_NM,
    S.ROW_NUM,
    S.INGEST_TS,
    S.BATCH_ID,
    S.SRC_ROW_HASH,
    'GATE_1_BRONZE_TO_SILVER',
    S.REJECT_CODE,
    S.REJECT_REASON,
    UUID_STRING(),
    NULL
);

-- ---------------------------------------------------------------------------
-- 5. Merge valid customers into Silver.
--    There may be multiple valid versions of one CUSTOMER_ID across files.
--    Choose exactly one: latest ingestion timestamp, then deterministic
--    tie-breakers. This guarantees one source row per Silver business key.
-- ---------------------------------------------------------------------------
MERGE INTO RETAIL_DB.TMP.SILVER_CUSTOMER_MASTER T
USING (
    SELECT
        CUSTOMER_ID,
        CUSTOMER_NAME,
        CITY,
        LOYALTY_TIER,
        REGISTRATION_DATE,
        ACTIVE_FLG,
        SRC_FILE_NM,
        ROW_NUM,
        SRC_ROW_HASH,
        SRC_SYS_C,
        BATCH_ID,
        INGEST_TS
    FROM (
        SELECT
            CUSTOMER_ID,
            CUSTOMER_NAME,
            CITY,
            LOYALTY_TIER,
            REGISTRATION_DATE,
            ACTIVE_FLG,
            SRC_FILE_NM,
            ROW_NUM,
            SRC_ROW_HASH,
            'RETAIL_CSV' AS SRC_SYS_C,
            BATCH_ID,
            INGEST_TS,
            ROW_NUMBER() OVER (
                PARTITION BY CUSTOMER_ID
                ORDER BY
                    INGEST_TS DESC NULLS LAST,
                    BATCH_ID DESC NULLS LAST,
                    SRC_FILE_NM DESC,
                    ROW_NUM DESC
            ) AS CUSTOMER_VERSION_RN
        FROM TMP_CUSTOMER_MASTER_CLASSIFIED
        WHERE REJECT_CODE IS NULL
    ) VALID_CUSTOMERS
    WHERE CUSTOMER_VERSION_RN = 1
) S
    ON T.CUSTOMER_ID = S.CUSTOMER_ID
WHEN MATCHED THEN UPDATE SET
    T.CUSTOMER_NAME = S.CUSTOMER_NAME,
    T.CITY = S.CITY,
    T.LOYALTY_TIER = S.LOYALTY_TIER,
    T.REGISTRATION_DATE = S.REGISTRATION_DATE,
    T.ACTIVE_FLG = S.ACTIVE_FLG,
    T.SRC_FILE_NM = S.SRC_FILE_NM,
    T.ROW_NUM = S.ROW_NUM,
    T.SRC_ROW_HASH = S.SRC_ROW_HASH,
    T.SRC_SYS_C = S.SRC_SYS_C,
    T.BATCH_ID = S.BATCH_ID,
    T.UPDATE_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CITY,
    LOYALTY_TIER,
    REGISTRATION_DATE,
    ACTIVE_FLG,
    SRC_FILE_NM,
    ROW_NUM,
    SRC_ROW_HASH,
    SRC_SYS_C,
    BATCH_ID,
    INSERT_TS,
    UPDATE_TS
) VALUES (
    S.CUSTOMER_ID,
    S.CUSTOMER_NAME,
    S.CITY,
    S.LOYALTY_TIER,
    S.REGISTRATION_DATE,
    S.ACTIVE_FLG,
    S.SRC_FILE_NM,
    S.ROW_NUM,
    S.SRC_ROW_HASH,
    S.SRC_SYS_C,
    S.BATCH_ID,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- 6. Complete only the load-control records created by this execution.
--    ROWS_LOADED counts valid source rows (before cross-file business-key
--    version selection); it is not the count of distinct Silver customers.
-- ---------------------------------------------------------------------------
UPDATE RETAIL_DB.TMP.SILVER_LOAD_CONTROL C
SET
    LOAD_STATUS = 'SUCCESS',
    COMPLETED_TS = CURRENT_TIMESTAMP(),
    ROWS_READ = A.ROWS_READ,
    ROWS_LOADED = A.ROWS_LOADED,
    ROWS_QUARANTINED = A.ROWS_QUARANTINED,
    ERROR_MESSAGE = NULL
FROM (
    SELECT
        CR.CONTROL_ID,
        COUNT(*) AS ROWS_READ,
        COUNT_IF(S.REJECT_CODE IS NULL) AS ROWS_LOADED,
        COUNT_IF(S.REJECT_CODE IS NOT NULL) AS ROWS_QUARANTINED
    FROM TMP_CUSTOMER_MASTER_CONTROL_ROWS CR
    INNER JOIN TMP_CUSTOMER_MASTER_CLASSIFIED S
        ON S.SRC_FILE_NM = CR.SRC_FILE_NM
    GROUP BY CR.CONTROL_ID
) A
WHERE C.CONTROL_ID = A.CONTROL_ID
  AND C.LOAD_STATUS = 'STARTED';

-- ---------------------------------------------------------------------------
-- 7. Validation output for manual execution.
--    These result sets are useful in Snowflake worksheets and harmless when
--    the DAG consumes the SQL stream.
-- ---------------------------------------------------------------------------
SELECT
    SRC_FILE_NM,
    COUNT(*) AS ROWS_READ,
    COUNT_IF(REJECT_CODE IS NULL) AS VALID_SOURCE_ROWS,
    COUNT_IF(REJECT_CODE IS NOT NULL) AS QUARANTINED_SOURCE_ROWS
FROM TMP_CUSTOMER_MASTER_CLASSIFIED
GROUP BY SRC_FILE_NM
ORDER BY SRC_FILE_NM;

SELECT
    COUNT(*) AS SILVER_ROWS,
    COUNT(DISTINCT CUSTOMER_ID) AS DISTINCT_CUSTOMERS
FROM RETAIL_DB.TMP.SILVER_CUSTOMER_MASTER;

SELECT
    SRC_FILE_NM,
    LOAD_STATUS,
    ROWS_READ,
    ROWS_LOADED,
    ROWS_QUARANTINED,
    ERROR_MESSAGE
FROM RETAIL_DB.TMP.SILVER_LOAD_CONTROL
WHERE CONTROL_ID IN (
    SELECT CONTROL_ID
    FROM TMP_CUSTOMER_MASTER_CONTROL_ROWS
)
ORDER BY SRC_FILE_NM;
