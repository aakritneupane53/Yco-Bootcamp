USE DATABASE RETAIL_DB;
USE SCHEMA TMP;
USE WAREHOUSE RETAIL_WH;

-- ---------------------------------------------------------------------------
-- 1. Normalize source strings and preserve raw source values for rejection.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE TMP_POS_SALES_CLASSIFIED AS
WITH normalized AS (
    SELECT
        NULLIF(UPPER(TRIM(TRANSACTION_ID)), '') AS TRANSACTION_ID,
        NULLIF(TRIM(LINE_NUMBER), '') AS LINE_NUMBER_RAW,
        NULLIF(TRIM(SALE_DATETIME), '') AS SALE_DATETIME_RAW,
        NULLIF(TRIM(BUSINESS_DATE), '') AS BUSINESS_DATE_RAW,
        NULLIF(UPPER(TRIM(STORE_ID)), '') AS STORE_ID,
        NULLIF(UPPER(TRIM(ITEM_ID)), '') AS ITEM_ID,
        NULLIF(UPPER(TRIM(CUSTOMER_ID)), '') AS CUSTOMER_ID,
        NULLIF(TRIM(SOLD_QTY), '') AS SOLD_QTY_RAW,
        NULLIF(TRIM(SELLING_PRICE_AMT), '') AS SELLING_PRICE_AMT_RAW,
        NULLIF(TRIM(DISCOUNT_AMT), '') AS DISCOUNT_AMT_RAW,
        NULLIF(TRIM(SALES_AMT), '') AS SALES_AMT_RAW,
        NULLIF(TRIM(COST_AMT), '') AS COST_AMT_RAW,

        -- Preserve the original raw source columns for rejection records.
        TRANSACTION_ID AS RAW_TRANSACTION_ID,
        LINE_NUMBER AS RAW_LINE_NUMBER,
        SALE_DATETIME AS RAW_SALE_DATETIME,
        BUSINESS_DATE AS RAW_BUSINESS_DATE,
        STORE_ID AS RAW_STORE_ID,
        ITEM_ID AS RAW_ITEM_ID,
        CUSTOMER_ID AS RAW_CUSTOMER_ID,
        SOLD_QTY AS RAW_SOLD_QTY,
        SELLING_PRICE_AMT AS RAW_SELLING_PRICE_AMT,
        DISCOUNT_AMT AS RAW_DISCOUNT_AMT,
        SALES_AMT AS RAW_SALES_AMT,
        COST_AMT AS RAW_COST_AMT,

        SRC_FILE_NM,
        ROW_NUM,
        INGEST_TS,
        BATCH_ID,

        SHA2(
            TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(
                'TRANSACTION_ID', TRANSACTION_ID,
                'LINE_NUMBER', LINE_NUMBER,
                'SALE_DATETIME', SALE_DATETIME,
                'BUSINESS_DATE', BUSINESS_DATE,
                'STORE_ID', STORE_ID,
                'ITEM_ID', ITEM_ID,
                'CUSTOMER_ID', CUSTOMER_ID,
                'SOLD_QTY', SOLD_QTY,
                'SELLING_PRICE_AMT', SELLING_PRICE_AMT,
                'DISCOUNT_AMT', DISCOUNT_AMT,
                'SALES_AMT', SALES_AMT,
                'COST_AMT', COST_AMT
            )),
            256
        ) AS SRC_ROW_HASH
    FROM RETAIL_DB.STG.STG_POS_SALES
),
typed AS (
    SELECT
        *,
        CASE
            WHEN REGEXP_LIKE(LINE_NUMBER_RAW, '^[0-9]+$')
                THEN TRY_TO_NUMBER(LINE_NUMBER_RAW, 38, 0)
            ELSE NULL
        END AS LINE_NUMBER,

        TRY_TO_TIMESTAMP_NTZ(
            SALE_DATETIME_RAW,
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) AS SALE_DATETIME,

        TRY_TO_DATE(
            BUSINESS_DATE_RAW,
            'YYYY-MM-DD'
        ) AS BUSINESS_DATE,

        TRY_TO_DECIMAL(SOLD_QTY_RAW, 18, 3) AS SOLD_QTY,
        TRY_TO_DECIMAL(SELLING_PRICE_AMT_RAW, 18, 2) AS SELLING_PRICE_AMT,
        TRY_TO_DECIMAL(DISCOUNT_AMT_RAW, 18, 2) AS DISCOUNT_AMT,
        TRY_TO_DECIMAL(SALES_AMT_RAW, 18, 2) AS SALES_AMT,
        TRY_TO_DECIMAL(COST_AMT_RAW, 18, 2) AS COST_AMT
    FROM normalized
),
hashed AS (
    SELECT
        *,
        SHA2(
            TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(
                'TRANSACTION_ID', TRANSACTION_ID,
                'LINE_NUMBER', LINE_NUMBER,
                'SALE_DATETIME', SALE_DATETIME,
                'BUSINESS_DATE', BUSINESS_DATE,
                'STORE_ID', STORE_ID,
                'ITEM_ID', ITEM_ID,
                'CUSTOMER_ID', CUSTOMER_ID,
                'SOLD_QTY', SOLD_QTY,
                'SELLING_PRICE_AMT', SELLING_PRICE_AMT,
                'DISCOUNT_AMT', DISCOUNT_AMT,
                'SALES_AMT', SALES_AMT,
                'COST_AMT', COST_AMT
            )),
            256
        ) AS NORMALIZED_PAYLOAD_HASH
    FROM typed
),
duplicate_stats AS (
    SELECT
        TRANSACTION_ID,
        LINE_NUMBER,
        COUNT(*) AS KEY_ROW_COUNT,
        COUNT(DISTINCT NORMALIZED_PAYLOAD_HASH) AS DISTINCT_PAYLOAD_COUNT
    FROM hashed
    WHERE TRANSACTION_ID IS NOT NULL
      AND LINE_NUMBER IS NOT NULL
    GROUP BY TRANSACTION_ID, LINE_NUMBER
),
ranked AS (
    SELECT
        H.*,
        COALESCE(D.KEY_ROW_COUNT, 1) AS KEY_ROW_COUNT,
        COALESCE(D.DISTINCT_PAYLOAD_COUNT, 1) AS DISTINCT_PAYLOAD_COUNT,
        ROW_NUMBER() OVER (
            PARTITION BY H.TRANSACTION_ID, H.LINE_NUMBER
            ORDER BY H.INGEST_TS, H.SRC_FILE_NM, H.ROW_NUM
        ) AS DUPLICATE_RN
    FROM hashed H
    LEFT JOIN duplicate_stats D
        ON H.TRANSACTION_ID = D.TRANSACTION_ID
       AND H.LINE_NUMBER = D.LINE_NUMBER
)
SELECT
    R.*,
    CASE
        WHEN R.TRANSACTION_ID IS NULL
            THEN 'MISSING_TRANSACTION_ID'
        WHEN R.LINE_NUMBER_RAW IS NULL
            THEN 'MISSING_LINE_NUMBER'
        WHEN R.LINE_NUMBER IS NULL
            THEN 'INVALID_LINE_NUMBER'
        WHEN R.BUSINESS_DATE_RAW IS NULL
            THEN 'MISSING_BUSINESS_DATE'
        WHEN R.BUSINESS_DATE IS NULL
            THEN 'INVALID_BUSINESS_DATE'
        WHEN R.STORE_ID IS NULL
            THEN 'MISSING_STORE_ID'
        WHEN R.ITEM_ID IS NULL
            THEN 'MISSING_ITEM_ID'
        WHEN NOT EXISTS (
            SELECT 1
            FROM RETAIL_DB.TMP.SILVER_STORE_MASTER M
            WHERE UPPER(TRIM(M.STORE_ID)) = R.STORE_ID
        )
            THEN 'UNKNOWN_STORE_ID'
        WHEN R.SALE_DATETIME_RAW IS NOT NULL
             AND R.SALE_DATETIME IS NULL
            THEN 'INVALID_SALE_DATETIME'
        WHEN R.SOLD_QTY_RAW IS NOT NULL AND R.SOLD_QTY IS NULL
            THEN 'INVALID_SOLD_QTY'
        WHEN R.SELLING_PRICE_AMT_RAW IS NOT NULL
             AND R.SELLING_PRICE_AMT IS NULL
            THEN 'INVALID_SELLING_PRICE_AMT'
        WHEN R.DISCOUNT_AMT_RAW IS NOT NULL AND R.DISCOUNT_AMT IS NULL
            THEN 'INVALID_DISCOUNT_AMT'
        WHEN R.SALES_AMT_RAW IS NOT NULL AND R.SALES_AMT IS NULL
            THEN 'INVALID_SALES_AMT'
        WHEN R.COST_AMT_RAW IS NOT NULL AND R.COST_AMT IS NULL
            THEN 'INVALID_COST_AMT'
        WHEN R.KEY_ROW_COUNT > 1 AND R.DISTINCT_PAYLOAD_COUNT > 1
            THEN 'CONFLICTING_DUPLICATE_TRANSACTION_LINE'
        WHEN R.KEY_ROW_COUNT > 1 AND R.DUPLICATE_RN > 1
            THEN 'DUPLICATE_TRANSACTION_LINE'
        ELSE NULL
    END AS REJECT_CODE,

    CASE
        WHEN R.TRANSACTION_ID IS NULL
            THEN 'TRANSACTION_ID is NULL, empty, or whitespace.'
        WHEN R.LINE_NUMBER_RAW IS NULL
            THEN 'LINE_NUMBER is required.'
        WHEN R.LINE_NUMBER IS NULL
            THEN 'LINE_NUMBER must contain a whole number.'
        WHEN R.BUSINESS_DATE_RAW IS NULL
            THEN 'BUSINESS_DATE is required.'
        WHEN R.BUSINESS_DATE IS NULL
            THEN 'BUSINESS_DATE could not be converted using YYYY-MM-DD.'
        WHEN R.STORE_ID IS NULL
            THEN 'STORE_ID is required.'
        WHEN R.ITEM_ID IS NULL
            THEN 'ITEM_ID is required.'
        WHEN NOT EXISTS (
            SELECT 1
            FROM RETAIL_DB.TMP.SILVER_STORE_MASTER M
            WHERE UPPER(TRIM(M.STORE_ID)) = R.STORE_ID
        )
            THEN 'STORE_ID does not exist in SILVER_STORE_MASTER; the POS row has an unknown store reference.'
        WHEN R.SALE_DATETIME_RAW IS NOT NULL
             AND R.SALE_DATETIME IS NULL
            THEN 'SALE_DATETIME could not be converted using YYYY-MM-DD"T"HH24:MI:SS.'
        WHEN R.SOLD_QTY_RAW IS NOT NULL AND R.SOLD_QTY IS NULL
            THEN 'SOLD_QTY could not be converted to NUMBER(18,3).'
        WHEN R.SELLING_PRICE_AMT_RAW IS NOT NULL
             AND R.SELLING_PRICE_AMT IS NULL
            THEN 'SELLING_PRICE_AMT could not be converted to NUMBER(18,2).'
        WHEN R.DISCOUNT_AMT_RAW IS NOT NULL AND R.DISCOUNT_AMT IS NULL
            THEN 'DISCOUNT_AMT could not be converted to NUMBER(18,2).'
        WHEN R.SALES_AMT_RAW IS NOT NULL AND R.SALES_AMT IS NULL
            THEN 'SALES_AMT could not be converted to NUMBER(18,2).'
        WHEN R.COST_AMT_RAW IS NOT NULL AND R.COST_AMT IS NULL
            THEN 'COST_AMT could not be converted to NUMBER(18,2).'
        WHEN R.KEY_ROW_COUNT > 1 AND R.DISTINCT_PAYLOAD_COUNT > 1
            THEN 'The same TRANSACTION_ID and LINE_NUMBER occur with different normalized transaction-line values.'
        WHEN R.KEY_ROW_COUNT > 1 AND R.DUPLICATE_RN > 1
            THEN 'Exact duplicate transaction line; retained the earliest source row.'
        ELSE NULL
    END AS REJECT_REASON
FROM ranked R;

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
    'STG_POS_SALES',
    'SILVER_POS_SALES',
    SRC_FILE_NM,
    MAX(BATCH_ID),
    NULL, -- Populate with the actual Airflow run ID when orchestrated.
    'STARTED',
    CURRENT_TIMESTAMP()
FROM TMP_POS_SALES_CLASSIFIED
GROUP BY SRC_FILE_NM;

-- ---------------------------------------------------------------------------
-- 3. Remove stale rejections for source rows that now pass validation.
-- ---------------------------------------------------------------------------
DELETE FROM RETAIL_DB.TMP.REJ_POS_SALES T
USING TMP_POS_SALES_CLASSIFIED S
WHERE T.SRC_FILE_NM = S.SRC_FILE_NM
  AND T.ROW_NUM = S.ROW_NUM
  AND T.QUALITY_GATE = 'GATE_1_BRONZE_TO_SILVER'
  AND S.REJECT_CODE IS NULL;

-- ---------------------------------------------------------------------------
-- 4. Upsert rejected source rows using source lineage + quality gate.
--    Raw source values are retained in the rejection table.
-- ---------------------------------------------------------------------------
MERGE INTO RETAIL_DB.TMP.REJ_POS_SALES T
USING (
    SELECT *
    FROM TMP_POS_SALES_CLASSIFIED
    WHERE REJECT_CODE IS NOT NULL
) S
    ON T.SRC_FILE_NM = S.SRC_FILE_NM
   AND T.ROW_NUM = S.ROW_NUM
   AND T.QUALITY_GATE = 'GATE_1_BRONZE_TO_SILVER'
WHEN MATCHED THEN UPDATE SET
    T.TRANSACTION_ID = S.RAW_TRANSACTION_ID,
    T.LINE_NUMBER = S.RAW_LINE_NUMBER,
    T.SALE_DATETIME = S.RAW_SALE_DATETIME,
    T.BUSINESS_DATE = S.RAW_BUSINESS_DATE,
    T.STORE_ID = S.RAW_STORE_ID,
    T.ITEM_ID = S.RAW_ITEM_ID,
    T.CUSTOMER_ID = S.RAW_CUSTOMER_ID,
    T.SOLD_QTY = S.RAW_SOLD_QTY,
    T.SELLING_PRICE_AMT = S.RAW_SELLING_PRICE_AMT,
    T.DISCOUNT_AMT = S.RAW_DISCOUNT_AMT,
    T.SALES_AMT = S.RAW_SALES_AMT,
    T.COST_AMT = S.RAW_COST_AMT,
    T.SOURCE_INGEST_TS = S.INGEST_TS,
    T.BATCH_ID = S.BATCH_ID,
    T.SRC_ROW_HASH = S.SRC_ROW_HASH,
    T.REJECT_CODE = S.REJECT_CODE,
    T.REJECT_REASON = S.REJECT_REASON
WHEN NOT MATCHED THEN INSERT (
    TRANSACTION_ID,
    LINE_NUMBER,
    SALE_DATETIME,
    BUSINESS_DATE,
    STORE_ID,
    ITEM_ID,
    CUSTOMER_ID,
    SOLD_QTY,
    SELLING_PRICE_AMT,
    DISCOUNT_AMT,
    SALES_AMT,
    COST_AMT,
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
    S.RAW_TRANSACTION_ID,
    S.RAW_LINE_NUMBER,
    S.RAW_SALE_DATETIME,
    S.RAW_BUSINESS_DATE,
    S.RAW_STORE_ID,
    S.RAW_ITEM_ID,
    S.RAW_CUSTOMER_ID,
    S.RAW_SOLD_QTY,
    S.RAW_SELLING_PRICE_AMT,
    S.RAW_DISCOUNT_AMT,
    S.RAW_SALES_AMT,
    S.RAW_COST_AMT,
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
-- 5. Remove previously loaded Silver rows that are now invalid.
--    Conflicting transaction-line duplicates must be removed as a grain;
--    unknown-store rows must not remain in Silver after being quarantined.
-- ---------------------------------------------------------------------------
DELETE FROM RETAIL_DB.TMP.SILVER_POS_SALES T
USING (
    SELECT DISTINCT TRANSACTION_ID, LINE_NUMBER
    FROM TMP_POS_SALES_CLASSIFIED
    WHERE REJECT_CODE IN (
        'CONFLICTING_DUPLICATE_TRANSACTION_LINE',
        'UNKNOWN_STORE_ID'
    )
      AND TRANSACTION_ID IS NOT NULL
      AND LINE_NUMBER IS NOT NULL
) C
WHERE T.TRANSACTION_ID = C.TRANSACTION_ID
  AND T.LINE_NUMBER = C.LINE_NUMBER;

-- ---------------------------------------------------------------------------
-- 6. Merge valid transaction lines into Silver.
-- ---------------------------------------------------------------------------
MERGE INTO RETAIL_DB.TMP.SILVER_POS_SALES T
USING (
    SELECT
        TRANSACTION_ID,
        LINE_NUMBER,
        SALE_DATETIME,
        BUSINESS_DATE,
        STORE_ID,
        ITEM_ID,
        CUSTOMER_ID,
        SOLD_QTY,
        SELLING_PRICE_AMT,
        DISCOUNT_AMT,
        SALES_AMT,
        COST_AMT,
        SRC_FILE_NM,
        ROW_NUM,
        SRC_ROW_HASH,
        BATCH_ID
    FROM TMP_POS_SALES_CLASSIFIED
    WHERE REJECT_CODE IS NULL
) S
    ON T.TRANSACTION_ID = S.TRANSACTION_ID
   AND T.LINE_NUMBER = S.LINE_NUMBER
WHEN MATCHED THEN UPDATE SET
    T.SALE_DATETIME = S.SALE_DATETIME,
    T.BUSINESS_DATE = S.BUSINESS_DATE,
    T.STORE_ID = S.STORE_ID,
    T.ITEM_ID = S.ITEM_ID,
    T.CUSTOMER_ID = S.CUSTOMER_ID,
    T.SOLD_QTY = S.SOLD_QTY,
    T.SELLING_PRICE_AMT = S.SELLING_PRICE_AMT,
    T.DISCOUNT_AMT = S.DISCOUNT_AMT,
    T.SALES_AMT = S.SALES_AMT,
    T.COST_AMT = S.COST_AMT,
    T.SRC_FILE_NM = S.SRC_FILE_NM,
    T.ROW_NUM = S.ROW_NUM,
    T.SRC_ROW_HASH = S.SRC_ROW_HASH,
    T.SRC_SYS_C = 'RETAIL_CSV',
    T.BATCH_ID = S.BATCH_ID,
    T.UPDATE_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    TRANSACTION_ID,
    LINE_NUMBER,
    SALE_DATETIME,
    BUSINESS_DATE,
    STORE_ID,
    ITEM_ID,
    CUSTOMER_ID,
    SOLD_QTY,
    SELLING_PRICE_AMT,
    DISCOUNT_AMT,
    SALES_AMT,
    COST_AMT,
    SRC_FILE_NM,
    ROW_NUM,
    SRC_ROW_HASH,
    SRC_SYS_C,
    BATCH_ID,
    INSERT_TS,
    UPDATE_TS
) VALUES (
    S.TRANSACTION_ID,
    S.LINE_NUMBER,
    S.SALE_DATETIME,
    S.BUSINESS_DATE,
    S.STORE_ID,
    S.ITEM_ID,
    S.CUSTOMER_ID,
    S.SOLD_QTY,
    S.SELLING_PRICE_AMT,
    S.DISCOUNT_AMT,
    S.SALES_AMT,
    S.COST_AMT,
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
    FROM TMP_POS_SALES_CLASSIFIED
    GROUP BY SRC_FILE_NM
) A
WHERE C.SOURCE_TABLE = 'STG_POS_SALES'
  AND C.TARGET_TABLE = 'SILVER_POS_SALES'
  AND C.SRC_FILE_NM = A.SRC_FILE_NM
  AND C.LOAD_STATUS = 'STARTED';

-- ---------------------------------------------------------------------------
-- 8. Post-run checks (execute separately).
-- ---------------------------------------------------------------------------
-- SELECT COUNT(*) AS SILVER_POS_SALES_COUNT
-- FROM RETAIL_DB.TMP.SILVER_POS_SALES;
--
-- SELECT
--     QUALITY_GATE,
--     REJECT_CODE,
--     COUNT(*) AS REJECTED_ROWS
-- FROM RETAIL_DB.TMP.REJ_POS_SALES
-- WHERE QUALITY_GATE = 'GATE_1_BRONZE_TO_SILVER'
-- GROUP BY QUALITY_GATE, REJECT_CODE
-- ORDER BY REJECT_CODE;
--
-- SELECT *
-- FROM RETAIL_DB.TMP.SILVER_LOAD_CONTROL
-- WHERE SOURCE_TABLE = 'STG_POS_SALES'
-- ORDER BY STARTED_TS DESC;
