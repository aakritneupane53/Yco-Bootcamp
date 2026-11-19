
USE DATABASE RETAIL_DB;
USE SCHEMA TMP;
use warehouse RETAIL_WH;

-- ---------------------------------------------------------------------------
-- 1. Normalize Bronze values and preserve the original values for rejection.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE TMP_INVENTORY_SNAPSHOT_CLASSIFIED AS
WITH normalized AS (
    SELECT
        NULLIF(TRIM(BUSINESS_DATE), '') AS BUSINESS_DATE_RAW,
        NULLIF(UPPER(TRIM(ITEM_ID)), '') AS ITEM_ID,
        NULLIF(UPPER(TRIM(STORE_ID)), '') AS STORE_ID,

        NULLIF(TRIM(ON_HAND_QTY), '') AS ON_HAND_QTY_RAW,
        NULLIF(TRIM(AVAILABLE_QTY), '') AS AVAILABLE_QTY_RAW,
        NULLIF(TRIM(RESERVED_QTY), '') AS RESERVED_QTY_RAW,
        NULLIF(TRIM(INVENTORY_VALUE_AMT), '') AS INVENTORY_VALUE_AMT_RAW,

        -- Preserve source values exactly as received for the rejection table.
        BUSINESS_DATE AS RAW_BUSINESS_DATE,
        ITEM_ID AS RAW_ITEM_ID,
        STORE_ID AS RAW_STORE_ID,
        ON_HAND_QTY AS RAW_ON_HAND_QTY,
        AVAILABLE_QTY AS RAW_AVAILABLE_QTY,
        RESERVED_QTY AS RAW_RESERVED_QTY,
        INVENTORY_VALUE_AMT AS RAW_INVENTORY_VALUE_AMT,

        SRC_FILE_NM,
        ROW_NUM,
        INGEST_TS,
        BATCH_ID,

        SHA2(
            TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(
                'BUSINESS_DATE', BUSINESS_DATE,
                'ITEM_ID', ITEM_ID,
                'STORE_ID', STORE_ID,
                'ON_HAND_QTY', ON_HAND_QTY,
                'AVAILABLE_QTY', AVAILABLE_QTY,
                'RESERVED_QTY', RESERVED_QTY,
                'INVENTORY_VALUE_AMT', INVENTORY_VALUE_AMT
            )),
            256
        ) AS SRC_ROW_HASH
    FROM RETAIL_DB.STG.STG_INVENTORY_SNAPSHOT
),
typed AS (
    SELECT
        *,
        TRY_TO_DATE(BUSINESS_DATE_RAW, 'YYYY-MM-DD') AS BUSINESS_DATE,
        TRY_TO_DECIMAL(ON_HAND_QTY_RAW, 18, 3) AS ON_HAND_QTY,
        TRY_TO_DECIMAL(AVAILABLE_QTY_RAW, 18, 3) AS AVAILABLE_QTY,
        TRY_TO_DECIMAL(RESERVED_QTY_RAW, 18, 3) AS RESERVED_QTY,
        TRY_TO_DECIMAL(INVENTORY_VALUE_AMT_RAW, 18, 2)
            AS INVENTORY_VALUE_AMT
    FROM normalized
),
hashed AS (
    SELECT
        *,
        SHA2(
            TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(
                'BUSINESS_DATE', BUSINESS_DATE,
                'ITEM_ID', ITEM_ID,
                'STORE_ID', STORE_ID,
                'ON_HAND_QTY', ON_HAND_QTY,
                'AVAILABLE_QTY', AVAILABLE_QTY,
                'RESERVED_QTY', RESERVED_QTY,
                'INVENTORY_VALUE_AMT', INVENTORY_VALUE_AMT
            )),
            256
        ) AS NORMALIZED_PAYLOAD_HASH
    FROM typed
),
duplicate_stats AS (
    SELECT
        BUSINESS_DATE,
        ITEM_ID,
        STORE_ID,
        COUNT(*) AS KEY_ROW_COUNT,
        COUNT(DISTINCT NORMALIZED_PAYLOAD_HASH) AS DISTINCT_PAYLOAD_COUNT
    FROM hashed
    WHERE BUSINESS_DATE IS NOT NULL
      AND ITEM_ID IS NOT NULL
      AND STORE_ID IS NOT NULL
    GROUP BY BUSINESS_DATE, ITEM_ID, STORE_ID
),
ranked AS (
    SELECT
        H.*,
        COALESCE(D.KEY_ROW_COUNT, 1) AS KEY_ROW_COUNT,
        COALESCE(D.DISTINCT_PAYLOAD_COUNT, 1) AS DISTINCT_PAYLOAD_COUNT,
        ROW_NUMBER() OVER (
            PARTITION BY H.BUSINESS_DATE, H.ITEM_ID, H.STORE_ID
            ORDER BY H.INGEST_TS, H.SRC_FILE_NM, H.ROW_NUM
        ) AS DUPLICATE_RN
    FROM hashed H
    LEFT JOIN duplicate_stats D
        ON H.BUSINESS_DATE = D.BUSINESS_DATE
       AND H.ITEM_ID = D.ITEM_ID
       AND H.STORE_ID = D.STORE_ID
)
SELECT
    *,
    CASE
        WHEN BUSINESS_DATE_RAW IS NULL
            THEN 'MISSING_BUSINESS_DATE'
        WHEN BUSINESS_DATE IS NULL
            THEN 'INVALID_BUSINESS_DATE'
        WHEN ITEM_ID IS NULL
            THEN 'MISSING_ITEM_ID'
        WHEN STORE_ID IS NULL
            THEN 'MISSING_STORE_ID'
        WHEN ON_HAND_QTY_RAW IS NOT NULL AND ON_HAND_QTY IS NULL
            THEN 'INVALID_ON_HAND_QTY'
        WHEN AVAILABLE_QTY_RAW IS NOT NULL AND AVAILABLE_QTY IS NULL
            THEN 'INVALID_AVAILABLE_QTY'
        WHEN RESERVED_QTY_RAW IS NOT NULL AND RESERVED_QTY IS NULL
            THEN 'INVALID_RESERVED_QTY'
        WHEN INVENTORY_VALUE_AMT_RAW IS NOT NULL
             AND INVENTORY_VALUE_AMT IS NULL
            THEN 'INVALID_INVENTORY_VALUE_AMT'
        WHEN KEY_ROW_COUNT > 1 AND DISTINCT_PAYLOAD_COUNT > 1
            THEN 'CONFLICTING_DUPLICATE_INVENTORY_SNAPSHOT'
        WHEN KEY_ROW_COUNT > 1 AND DUPLICATE_RN > 1
            THEN 'DUPLICATE_INVENTORY_SNAPSHOT'
        ELSE NULL
    END AS REJECT_CODE,
    CASE
        WHEN BUSINESS_DATE_RAW IS NULL
            THEN 'BUSINESS_DATE is required.'
        WHEN BUSINESS_DATE IS NULL
            THEN 'BUSINESS_DATE could not be converted using YYYY-MM-DD.'
        WHEN ITEM_ID IS NULL
            THEN 'ITEM_ID is required.'
        WHEN STORE_ID IS NULL
            THEN 'STORE_ID is required.'
        WHEN ON_HAND_QTY_RAW IS NOT NULL AND ON_HAND_QTY IS NULL
            THEN 'ON_HAND_QTY could not be converted to NUMBER(18,3).'
        WHEN AVAILABLE_QTY_RAW IS NOT NULL AND AVAILABLE_QTY IS NULL
            THEN 'AVAILABLE_QTY could not be converted to NUMBER(18,3).'
        WHEN RESERVED_QTY_RAW IS NOT NULL AND RESERVED_QTY IS NULL
            THEN 'RESERVED_QTY could not be converted to NUMBER(18,3).'
        WHEN INVENTORY_VALUE_AMT_RAW IS NOT NULL
             AND INVENTORY_VALUE_AMT IS NULL
            THEN 'INVENTORY_VALUE_AMT could not be converted to NUMBER(18,2).'
        WHEN KEY_ROW_COUNT > 1 AND DISTINCT_PAYLOAD_COUNT > 1
            THEN 'The same BUSINESS_DATE, ITEM_ID, and STORE_ID occur with different normalized inventory values.'
        WHEN KEY_ROW_COUNT > 1 AND DUPLICATE_RN > 1
            THEN 'Exact duplicate inventory snapshot; retained the earliest source row.'
        ELSE NULL
    END AS REJECT_REASON
FROM ranked;

-- ---------------------------------------------------------------------------
-- 2. Start one load-control audit record per source file for this execution.
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
    'STG_INVENTORY_SNAPSHOT',
    'SILVER_INVENTORY_SNAPSHOT',
    SRC_FILE_NM,
    MAX(BATCH_ID),
    NULL, -- Populate with the actual Airflow run ID when orchestrated.
    'STARTED',
    CURRENT_TIMESTAMP()
FROM TMP_INVENTORY_SNAPSHOT_CLASSIFIED
GROUP BY SRC_FILE_NM;

-- ---------------------------------------------------------------------------
-- 3. Remove stale rejections for source rows that now pass validation.
-- ---------------------------------------------------------------------------
DELETE FROM RETAIL_DB.TMP.REJ_INVENTORY_SNAPSHOT T
USING TMP_INVENTORY_SNAPSHOT_CLASSIFIED S
WHERE T.SRC_FILE_NM = S.SRC_FILE_NM
  AND T.ROW_NUM = S.ROW_NUM
  AND T.QUALITY_GATE = 'GATE_1_BRONZE_TO_SILVER'
  AND S.REJECT_CODE IS NULL;

-- ---------------------------------------------------------------------------
-- 4. Upsert rejected source rows. Raw source values are retained.
--    The merge key makes rejection handling idempotent by source lineage.
-- ---------------------------------------------------------------------------
MERGE INTO RETAIL_DB.TMP.REJ_INVENTORY_SNAPSHOT T
USING (
    SELECT *
    FROM TMP_INVENTORY_SNAPSHOT_CLASSIFIED
    WHERE REJECT_CODE IS NOT NULL
) S
    ON T.SRC_FILE_NM = S.SRC_FILE_NM
   AND T.ROW_NUM = S.ROW_NUM
   AND T.QUALITY_GATE = 'GATE_1_BRONZE_TO_SILVER'
WHEN MATCHED THEN UPDATE SET
    T.BUSINESS_DATE = S.RAW_BUSINESS_DATE,
    T.ITEM_ID = S.RAW_ITEM_ID,
    T.STORE_ID = S.RAW_STORE_ID,
    T.ON_HAND_QTY = S.RAW_ON_HAND_QTY,
    T.AVAILABLE_QTY = S.RAW_AVAILABLE_QTY,
    T.RESERVED_QTY = S.RAW_RESERVED_QTY,
    T.INVENTORY_VALUE_AMT = S.RAW_INVENTORY_VALUE_AMT,
    T.SOURCE_INGEST_TS = S.INGEST_TS,
    T.BATCH_ID = S.BATCH_ID,
    T.SRC_ROW_HASH = S.SRC_ROW_HASH,
    T.REJECT_CODE = S.REJECT_CODE,
    T.REJECT_REASON = S.REJECT_REASON
WHEN NOT MATCHED THEN INSERT (
    BUSINESS_DATE,
    ITEM_ID,
    STORE_ID,
    ON_HAND_QTY,
    AVAILABLE_QTY,
    RESERVED_QTY,
    INVENTORY_VALUE_AMT,
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
    S.RAW_BUSINESS_DATE,
    S.RAW_ITEM_ID,
    S.RAW_STORE_ID,
    S.RAW_ON_HAND_QTY,
    S.RAW_AVAILABLE_QTY,
    S.RAW_RESERVED_QTY,
    S.RAW_INVENTORY_VALUE_AMT,
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
-- 5. Remove previously loaded Silver rows for conflicting composite keys.
--    This prevents an older version from surviving after a conflict is found.
-- ---------------------------------------------------------------------------
DELETE FROM RETAIL_DB.TMP.SILVER_INVENTORY_SNAPSHOT T
USING (
    SELECT DISTINCT
        BUSINESS_DATE,
        ITEM_ID,
        STORE_ID
    FROM TMP_INVENTORY_SNAPSHOT_CLASSIFIED
    WHERE REJECT_CODE = 'CONFLICTING_DUPLICATE_INVENTORY_SNAPSHOT'
      AND BUSINESS_DATE IS NOT NULL
      AND ITEM_ID IS NOT NULL
      AND STORE_ID IS NOT NULL
) C
WHERE T.BUSINESS_DATE = C.BUSINESS_DATE
  AND T.ITEM_ID = C.ITEM_ID
  AND T.STORE_ID = C.STORE_ID;

-- ---------------------------------------------------------------------------
-- 6. Merge valid daily snapshots into Silver using the composite business key.
-- ---------------------------------------------------------------------------
MERGE INTO RETAIL_DB.TMP.SILVER_INVENTORY_SNAPSHOT T
USING (
    SELECT
        BUSINESS_DATE,
        ITEM_ID,
        STORE_ID,
        ON_HAND_QTY,
        AVAILABLE_QTY,
        RESERVED_QTY,
        INVENTORY_VALUE_AMT,
        SRC_FILE_NM,
        ROW_NUM,
        SRC_ROW_HASH,
        BATCH_ID
    FROM TMP_INVENTORY_SNAPSHOT_CLASSIFIED
    WHERE REJECT_CODE IS NULL
) S
    ON T.BUSINESS_DATE = S.BUSINESS_DATE
   AND T.ITEM_ID = S.ITEM_ID
   AND T.STORE_ID = S.STORE_ID
WHEN MATCHED THEN UPDATE SET
    T.ON_HAND_QTY = S.ON_HAND_QTY,
    T.AVAILABLE_QTY = S.AVAILABLE_QTY,
    T.RESERVED_QTY = S.RESERVED_QTY,
    T.INVENTORY_VALUE_AMT = S.INVENTORY_VALUE_AMT,
    T.SRC_FILE_NM = S.SRC_FILE_NM,
    T.ROW_NUM = S.ROW_NUM,
    T.SRC_ROW_HASH = S.SRC_ROW_HASH,
    T.SRC_SYS_C = 'RETAIL_CSV',
    T.BATCH_ID = S.BATCH_ID,
    T.UPDATE_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    BUSINESS_DATE,
    ITEM_ID,
    STORE_ID,
    ON_HAND_QTY,
    AVAILABLE_QTY,
    RESERVED_QTY,
    INVENTORY_VALUE_AMT,
    SRC_FILE_NM,
    ROW_NUM,
    SRC_ROW_HASH,
    SRC_SYS_C,
    BATCH_ID,
    INSERT_TS,
    UPDATE_TS
) VALUES (
    S.BUSINESS_DATE,
    S.ITEM_ID,
    S.STORE_ID,
    S.ON_HAND_QTY,
    S.AVAILABLE_QTY,
    S.RESERVED_QTY,
    S.INVENTORY_VALUE_AMT,
    S.SRC_FILE_NM,
    S.ROW_NUM,
    S.SRC_ROW_HASH,
    'RETAIL_CSV',
    S.BATCH_ID,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- 7. Complete the load-control audit for each source file.
--    ROWS_LOADED counts valid source rows presented to the Silver MERGE.
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
    FROM TMP_INVENTORY_SNAPSHOT_CLASSIFIED
    GROUP BY SRC_FILE_NM
) A
WHERE C.SOURCE_TABLE = 'STG_INVENTORY_SNAPSHOT'
  AND C.TARGET_TABLE = 'SILVER_INVENTORY_SNAPSHOT'
  AND C.SRC_FILE_NM = A.SRC_FILE_NM
  AND C.LOAD_STATUS = 'STARTED';

-- ---------------------------------------------------------------------------
-- 8. Post-run checks (execute separately).
-- ---------------------------------------------------------------------------
-- SELECT COUNT(*) AS SILVER_INVENTORY_SNAPSHOT_COUNT
-- FROM RETAIL_DB.TMP.SILVER_INVENTORY_SNAPSHOT;
--
-- SELECT
--     REJECT_CODE,
--     COUNT(*) AS REJECTED_ROWS
-- FROM RETAIL_DB.TMP.REJ_INVENTORY_SNAPSHOT
-- WHERE QUALITY_GATE = 'GATE_1_BRONZE_TO_SILVER'
-- GROUP BY REJECT_CODE
-- ORDER BY REJECT_CODE;
--
-- SELECT *
-- FROM RETAIL_DB.TMP.SILVER_LOAD_CONTROL
-- WHERE SOURCE_TABLE = 'STG_INVENTORY_SNAPSHOT'
-- ORDER BY STARTED_TS DESC;
