
USE DATABASE RETAIL_DB;
USE SCHEMA TMP;
use warehouse RETAIL_WH;

-- ---------------------------------------------------------------------------
-- 1. Standardize, type-cast, and classify Bronze rows.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE TMP_CUSTOMER_MASTER_CLASSIFIED AS
WITH normalized AS (
    SELECT
        NULLIF(UPPER(TRIM(CUSTOMER_ID)), '') AS CUSTOMER_ID,
        NULLIF(TRIM(CUSTOMER_NAME), '') AS CUSTOMER_NAME,
        NULLIF(TRIM(CITY), '') AS CITY,
        NULLIF(UPPER(TRIM(LOYALTY_TIER)), '') AS LOYALTY_TIER,
        NULLIF(TRIM(REGISTRATION_DATE), '') AS REGISTRATION_DATE_RAW,
        CASE
            WHEN UPPER(TRIM(ACTIVE_FLG)) IN ('1', 'TRUE', 'Y') THEN TRUE
            WHEN UPPER(TRIM(ACTIVE_FLG)) IN ('0', 'FALSE', 'N') THEN FALSE
            ELSE NULL
        END AS ACTIVE_FLG,
        NULLIF(TRIM(ACTIVE_FLG), '') AS ACTIVE_FLG_RAW,
        SRC_FILE_NM,
        ROW_NUM,
        INGEST_TS,
        BATCH_ID,
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
        ) AS SRC_ROW_HASH
    FROM RETAIL_DB.STG.STG_CUSTOMER_MASTER
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
            ORDER BY H.ROW_NUM
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
            THEN 'CUSTOMER_ID occurs more than once in the source file with different normalized attributes.'
        WHEN KEY_ROW_COUNT > 1 AND DUPLICATE_RN > 1
            THEN 'Exact duplicate CUSTOMER_ID record within the same source file; retained the first row.'
        ELSE NULL
    END AS REJECT_REASON
FROM ranked;

-- ---------------------------------------------------------------------------
-- 2. Start one audit record per source file for this execution.
-- ---------------------------------------------------------------------------
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
    UUID_STRING(),
    'STG_CUSTOMER_MASTER',
    'SILVER_CUSTOMER_MASTER',
    SRC_FILE_NM,
    MAX(BATCH_ID),
    NULL, -- Populate with the actual Airflow run ID when orchestrated.
    'STARTED',
    CURRENT_TIMESTAMP()
FROM TMP_CUSTOMER_MASTER_CLASSIFIED
GROUP BY SRC_FILE_NM;

-- ---------------------------------------------------------------------------
-- 3. Remove stale rejections for source rows that now pass validation.
--    This specifically cleans up rows wrongly rejected by the previous
--    MM/DD/YYYY date mask. Only this source file/row/gate is affected.
-- ---------------------------------------------------------------------------
DELETE FROM RETAIL_DB.TMP.REJ_CUSTOMER_MASTER T
USING TMP_CUSTOMER_MASTER_CLASSIFIED S
WHERE T.SRC_FILE_NM = S.SRC_FILE_NM
  AND T.ROW_NUM = S.ROW_NUM
  AND T.QUALITY_GATE = 'GATE_1_BRONZE_TO_SILVER'
  AND S.REJECT_CODE IS NULL;

-- ---------------------------------------------------------------------------
-- 4. Upsert current rejected rows. Merge key makes rejection handling
--    idempotent for each source file + row number + quality gate.
-- ---------------------------------------------------------------------------
MERGE INTO RETAIL_DB.TMP.REJ_CUSTOMER_MASTER T
USING (
    SELECT *
    FROM TMP_CUSTOMER_MASTER_CLASSIFIED
    WHERE REJECT_CODE IS NOT NULL
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
-- 5. Merge valid customer records into Silver.
--    CUSTOMER_ID is the business key because Silver has no snapshot date.
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
        BATCH_ID
    FROM TMP_CUSTOMER_MASTER_CLASSIFIED
    WHERE REJECT_CODE IS NULL
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
    T.SRC_SYS_C = 'RETAIL_CSV',
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
    'RETAIL_CSV',
    S.BATCH_ID,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- 6. Complete the audit record created in step 2.
-- ---------------------------------------------------------------------------
UPDATE RETAIL_DB.TMP.SILVER_LOAD_CONTROL C
SET
    LOAD_STATUS = 'SUCCESS',
    COMPLETED_TS = CURRENT_TIMESTAMP(),
    ROWS_READ = A.ROWS_READ,
    ROWS_LOADED = A.ROWS_LOADED,
    ROWS_QUARANTINED = A.ROWS_QUARANTINED
FROM (
    SELECT
        SRC_FILE_NM,
        COUNT(*) AS ROWS_READ,
        COUNT_IF(REJECT_CODE IS NULL) AS ROWS_LOADED,
        COUNT_IF(REJECT_CODE IS NOT NULL) AS ROWS_QUARANTINED
    FROM TMP_CUSTOMER_MASTER_CLASSIFIED
    GROUP BY SRC_FILE_NM
) A
WHERE C.SOURCE_TABLE = 'STG_CUSTOMER_MASTER'
  AND C.TARGET_TABLE = 'SILVER_CUSTOMER_MASTER'
  AND C.SRC_FILE_NM = A.SRC_FILE_NM
  AND C.LOAD_STATUS = 'STARTED';

