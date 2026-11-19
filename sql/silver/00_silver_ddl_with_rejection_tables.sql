-- ============================================================
-- RETAIL MEDALLION PROJECT
-- Silver layer object definitions
-- Database : RETAIL_DB
-- Source   : RETAIL_DB.STG
-- ============================================================
-- Design notes:
--   1. Silver tables are clean, typed and source-grain oriented.
--   2. SNAPSHOT_DT for item/store master is derived from the source
--      filename (YYYYMMDD), because it is not a CSV data column.
--   3. SRC_FILE_NM + ROW_NUM is the source-row lineage key used by
--      MERGE statements to make Silver loads idempotent.
--   4. The DDL creates structures only. Validation, quarantine
--      routing and MERGE logic belong in separate SQL scripts.
-- ============================================================

USE DATABASE RETAIL_DB;


USE SCHEMA TMP;

-- ------------------------------------------------------------
-- 1. ITEM MASTER
-- Grain: one item per source snapshot row.
-- SNAPSHOT_DT is parsed from item_master_YYYYMMDD.csv.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS SILVER_ITEM_MASTER (
    ITEM_ID             VARCHAR NOT NULL,
    ITEM_NAME           VARCHAR,
    SUBCATEGORY_ID      VARCHAR,
    SUBCATEGORY_NAME    VARCHAR,
    CATEGORY_ID         VARCHAR,
    CATEGORY_NAME       VARCHAR,
    BRAND_ID            VARCHAR,
    BRAND_NAME          VARCHAR,
    UNIT_OF_MEASURE     VARCHAR,
    ACTIVE_FLG          BOOLEAN,
    SNAPSHOT_DT         DATE NOT NULL,

    SRC_FILE_NM         VARCHAR(1000) NOT NULL,
    ROW_NUM             NUMBER(38,0) NOT NULL,
    SRC_ROW_HASH        VARCHAR(64),
    SRC_SYS_C           VARCHAR(100) DEFAULT 'RETAIL_CSV',
    BATCH_ID            VARCHAR(100),
    INSERT_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATE_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Typed item master snapshots; preserves daily source history';

-- ------------------------------------------------------------
-- 2. STORE MASTER
-- Grain: one store per source snapshot row.
-- SNAPSHOT_DT is parsed from store_master_YYYYMMDD.csv.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS SILVER_STORE_MASTER (
    STORE_ID            VARCHAR NOT NULL,
    STORE_NAME          VARCHAR,
    REGION_ID           VARCHAR,
    REGION_NAME         VARCHAR,
    COUNTRY_ID          VARCHAR,
    COUNTRY_NAME        VARCHAR,
    CITY                VARCHAR,
    STORE_TYPE          VARCHAR,
    ACTIVE_FLG          BOOLEAN,
    SNAPSHOT_DT         DATE NOT NULL,

    SRC_FILE_NM         VARCHAR(1000) NOT NULL,
    ROW_NUM             NUMBER(38,0) NOT NULL,
    SRC_ROW_HASH        VARCHAR(64),
    SRC_SYS_C           VARCHAR(100) DEFAULT 'RETAIL_CSV',
    BATCH_ID            VARCHAR(100),
    INSERT_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATE_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Typed store master snapshots; preserves weekly source history';

-- ------------------------------------------------------------
-- 3. CUSTOMER MASTER
-- Grain: one customer per source row.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS SILVER_CUSTOMER_MASTER (
    CUSTOMER_ID         VARCHAR NOT NULL,
    CUSTOMER_NAME       VARCHAR,
    CITY                VARCHAR,
    LOYALTY_TIER        VARCHAR,
    REGISTRATION_DATE   DATE,
    ACTIVE_FLG          BOOLEAN,

    SRC_FILE_NM         VARCHAR(1000) NOT NULL,
    ROW_NUM             NUMBER(38,0) NOT NULL,
    SRC_ROW_HASH        VARCHAR(64),
    SRC_SYS_C           VARCHAR(100) DEFAULT 'RETAIL_CSV',
    BATCH_ID            VARCHAR(100),
    INSERT_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATE_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Typed customer reference data';

-- ------------------------------------------------------------
-- 4. POS SALES
-- Grain: one transaction line per source row.
-- CUSTOMER_ID is nullable to allow anonymous/non-loyalty sales.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS SILVER_POS_SALES (
    TRANSACTION_ID      VARCHAR NOT NULL,
    LINE_NUMBER         NUMBER(38,0) NOT NULL,
    SALE_DATETIME       TIMESTAMP_NTZ,
    BUSINESS_DATE       DATE NOT NULL,
    STORE_ID            VARCHAR NOT NULL,
    ITEM_ID             VARCHAR NOT NULL,
    CUSTOMER_ID         VARCHAR,
    SOLD_QTY            NUMBER(18,3),
    SELLING_PRICE_AMT   NUMBER(18,2),
    DISCOUNT_AMT        NUMBER(18,2),
    SALES_AMT           NUMBER(18,2),
    COST_AMT            NUMBER(18,2),

    SRC_FILE_NM         VARCHAR(1000) NOT NULL,
    ROW_NUM             NUMBER(38,0) NOT NULL,
    SRC_ROW_HASH        VARCHAR(64),
    SRC_SYS_C           VARCHAR(100) DEFAULT 'RETAIL_CSV',
    BATCH_ID            VARCHAR(100),
    INSERT_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATE_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Typed POS transaction-line data';

-- ------------------------------------------------------------
-- 5. INVENTORY SNAPSHOT
-- Grain: one item, store and business date per source row.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS SILVER_INVENTORY_SNAPSHOT (
    BUSINESS_DATE       DATE NOT NULL,
    ITEM_ID             VARCHAR NOT NULL,
    STORE_ID            VARCHAR NOT NULL,
    ON_HAND_QTY         NUMBER(18,3),
    AVAILABLE_QTY       NUMBER(18,3),
    RESERVED_QTY        NUMBER(18,3),
    INVENTORY_VALUE_AMT NUMBER(18,2),

    SRC_FILE_NM         VARCHAR(1000) NOT NULL,
    ROW_NUM             NUMBER(38,0) NOT NULL,
    SRC_ROW_HASH        VARCHAR(64),
    SRC_SYS_C           VARCHAR(100) DEFAULT 'RETAIL_CSV',
    BATCH_ID            VARCHAR(100),
    INSERT_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATE_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Typed daily item/store inventory snapshots';

-- ------------------------------------------------------------
-- 6. PRICE SNAPSHOT
-- Grain: one item, store and business date per source row.
-- Gold FCT_PRICE effective periods are derived downstream.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS SILVER_PRICE_SNAPSHOT (
    BUSINESS_DATE       DATE NOT NULL,
    ITEM_ID             VARCHAR NOT NULL,
    STORE_ID            VARCHAR NOT NULL,
    SELLING_PRICE_AMT   NUMBER(18,2),
    UNIT_COST_AMT       NUMBER(18,2),

    SRC_FILE_NM         VARCHAR(1000) NOT NULL,
    ROW_NUM             NUMBER(38,0) NOT NULL,
    SRC_ROW_HASH        VARCHAR(64),
    SRC_SYS_C           VARCHAR(100) DEFAULT 'RETAIL_CSV',
    BATCH_ID            VARCHAR(100),
    INSERT_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATE_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Typed daily item/store price snapshots';

-- ------------------------------------------------------------
-- 7. TABLE-SPECIFIC REJECTION TABLES
-- One rejection table per Silver source table.
--
-- Design:
--   * Source columns remain VARCHAR to preserve the original Bronze
--     values exactly, including values that failed type conversion.
--   * QUALITY_GATE identifies the stage that rejected the row:
--       GATE_1_BRONZE_TO_SILVER
--       GATE_2_REFERENCE_VALIDATION
--       GATE_3_SILVER_TO_GOLD
--   * Store all applicable reasons for a row/gate in REJECT_REASON.
--   * For idempotent rejection writes, MERGE using
--     SRC_FILE_NM + ROW_NUM + QUALITY_GATE. A re-run should update the
--     existing rejection rather than append another copy.
--   * Keep the same source row eligible for a separate rejection event
--     at a different quality gate, if applicable.
-- ------------------------------------------------------------

-- 7.1 ITEM MASTER REJECTIONS
CREATE TABLE IF NOT EXISTS REJ_ITEM_MASTER (
    ITEM_ID             VARCHAR,
    ITEM_NAME           VARCHAR,
    SUBCATEGORY_ID      VARCHAR,
    SUBCATEGORY_NAME    VARCHAR,
    CATEGORY_ID         VARCHAR,
    CATEGORY_NAME       VARCHAR,
    BRAND_ID            VARCHAR,
    BRAND_NAME          VARCHAR,
    UNIT_OF_MEASURE     VARCHAR,
    ACTIVE_FLG          VARCHAR,

    SRC_FILE_NM         VARCHAR(1000) NOT NULL,
    ROW_NUM             NUMBER(38,0) NOT NULL,
    SOURCE_INGEST_TS    TIMESTAMP_NTZ,
    BATCH_ID            VARCHAR(100),
    SRC_ROW_HASH        VARCHAR(64),

    QUALITY_GATE        VARCHAR(50) NOT NULL,
    REJECT_CODE         VARCHAR(100),
    REJECT_REASON       VARCHAR(4000) NOT NULL,
    REJECTION_ID        VARCHAR(36) NOT NULL,
    AIRFLOW_RUN_ID      VARCHAR(250),
    REJECTED_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RESOLVED_FLG        BOOLEAN DEFAULT FALSE,
    RESOLVED_TS         TIMESTAMP_NTZ
)
COMMENT = 'Rejected item master source rows; raw source values are retained as VARCHAR';

-- 7.2 STORE MASTER REJECTIONS
CREATE TABLE IF NOT EXISTS REJ_STORE_MASTER (
    STORE_ID            VARCHAR,
    STORE_NAME          VARCHAR,
    REGION_ID           VARCHAR,
    REGION_NAME         VARCHAR,
    COUNTRY_ID          VARCHAR,
    COUNTRY_NAME        VARCHAR,
    CITY                VARCHAR,
    STORE_TYPE          VARCHAR,
    ACTIVE_FLG          VARCHAR,

    SRC_FILE_NM         VARCHAR(1000) NOT NULL,
    ROW_NUM             NUMBER(38,0) NOT NULL,
    SOURCE_INGEST_TS    TIMESTAMP_NTZ,
    BATCH_ID            VARCHAR(100),
    SRC_ROW_HASH        VARCHAR(64),

    QUALITY_GATE        VARCHAR(50) NOT NULL,
    REJECT_CODE         VARCHAR(100),
    REJECT_REASON       VARCHAR(4000) NOT NULL,
    REJECTION_ID        VARCHAR(36) NOT NULL,
    AIRFLOW_RUN_ID      VARCHAR(250),
    REJECTED_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RESOLVED_FLG        BOOLEAN DEFAULT FALSE,
    RESOLVED_TS         TIMESTAMP_NTZ
)
COMMENT = 'Rejected store master source rows; raw source values are retained as VARCHAR';

-- 7.3 CUSTOMER MASTER REJECTIONS
CREATE TABLE IF NOT EXISTS REJ_CUSTOMER_MASTER (
    CUSTOMER_ID         VARCHAR,
    CUSTOMER_NAME       VARCHAR,
    CITY                VARCHAR,
    LOYALTY_TIER        VARCHAR,
    REGISTRATION_DATE   VARCHAR,
    ACTIVE_FLG          VARCHAR,

    SRC_FILE_NM         VARCHAR(1000) NOT NULL,
    ROW_NUM             NUMBER(38,0) NOT NULL,
    SOURCE_INGEST_TS    TIMESTAMP_NTZ,
    BATCH_ID            VARCHAR(100),
    SRC_ROW_HASH        VARCHAR(64),

    QUALITY_GATE        VARCHAR(50) NOT NULL,
    REJECT_CODE         VARCHAR(100),
    REJECT_REASON       VARCHAR(4000) NOT NULL,
    REJECTION_ID        VARCHAR(36) NOT NULL,
    AIRFLOW_RUN_ID      VARCHAR(250),
    REJECTED_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RESOLVED_FLG        BOOLEAN DEFAULT FALSE,
    RESOLVED_TS         TIMESTAMP_NTZ
)
COMMENT = 'Rejected customer master source rows; raw source values are retained as VARCHAR';

-- 7.4 POS SALES REJECTIONS
CREATE TABLE IF NOT EXISTS REJ_POS_SALES (
    TRANSACTION_ID      VARCHAR,
    LINE_NUMBER         VARCHAR,
    SALE_DATETIME       VARCHAR,
    BUSINESS_DATE       VARCHAR,
    STORE_ID            VARCHAR,
    ITEM_ID             VARCHAR,
    CUSTOMER_ID         VARCHAR,
    SOLD_QTY            VARCHAR,
    SELLING_PRICE_AMT   VARCHAR,
    DISCOUNT_AMT        VARCHAR,
    SALES_AMT           VARCHAR,
    COST_AMT            VARCHAR,

    SRC_FILE_NM         VARCHAR(1000) NOT NULL,
    ROW_NUM             NUMBER(38,0) NOT NULL,
    SOURCE_INGEST_TS    TIMESTAMP_NTZ,
    BATCH_ID            VARCHAR(100),
    SRC_ROW_HASH        VARCHAR(64),

    QUALITY_GATE        VARCHAR(50) NOT NULL,
    REJECT_CODE         VARCHAR(100),
    REJECT_REASON       VARCHAR(4000) NOT NULL,
    REJECTION_ID        VARCHAR(36) NOT NULL,
    AIRFLOW_RUN_ID      VARCHAR(250),
    REJECTED_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RESOLVED_FLG        BOOLEAN DEFAULT FALSE,
    RESOLVED_TS         TIMESTAMP_NTZ
)
COMMENT = 'Rejected POS sales source rows; raw source values are retained as VARCHAR';

-- 7.5 INVENTORY SNAPSHOT REJECTIONS
CREATE TABLE IF NOT EXISTS REJ_INVENTORY_SNAPSHOT (
    BUSINESS_DATE       VARCHAR,
    ITEM_ID             VARCHAR,
    STORE_ID            VARCHAR,
    ON_HAND_QTY         VARCHAR,
    AVAILABLE_QTY       VARCHAR,
    RESERVED_QTY        VARCHAR,
    INVENTORY_VALUE_AMT VARCHAR,

    SRC_FILE_NM         VARCHAR(1000) NOT NULL,
    ROW_NUM             NUMBER(38,0) NOT NULL,
    SOURCE_INGEST_TS    TIMESTAMP_NTZ,
    BATCH_ID            VARCHAR(100),
    SRC_ROW_HASH        VARCHAR(64),

    QUALITY_GATE        VARCHAR(50) NOT NULL,
    REJECT_CODE         VARCHAR(100),
    REJECT_REASON       VARCHAR(4000) NOT NULL,
    REJECTION_ID        VARCHAR(36) NOT NULL,
    AIRFLOW_RUN_ID      VARCHAR(250),
    REJECTED_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RESOLVED_FLG        BOOLEAN DEFAULT FALSE,
    RESOLVED_TS         TIMESTAMP_NTZ
)
COMMENT = 'Rejected inventory snapshot source rows; raw source values are retained as VARCHAR';

-- 7.6 PRICE SNAPSHOT REJECTIONS
CREATE TABLE IF NOT EXISTS REJ_PRICE_SNAPSHOT (
    BUSINESS_DATE       VARCHAR,
    ITEM_ID             VARCHAR,
    STORE_ID            VARCHAR,
    SELLING_PRICE_AMT   VARCHAR,
    UNIT_COST_AMT       VARCHAR,

    SRC_FILE_NM         VARCHAR(1000) NOT NULL,
    ROW_NUM             NUMBER(38,0) NOT NULL,
    SOURCE_INGEST_TS    TIMESTAMP_NTZ,
    BATCH_ID            VARCHAR(100),
    SRC_ROW_HASH        VARCHAR(64),

    QUALITY_GATE        VARCHAR(50) NOT NULL,
    REJECT_CODE         VARCHAR(100),
    REJECT_REASON       VARCHAR(4000) NOT NULL,
    REJECTION_ID        VARCHAR(36) NOT NULL,
    AIRFLOW_RUN_ID      VARCHAR(250),
    REJECTED_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RESOLVED_FLG        BOOLEAN DEFAULT FALSE,
    RESOLVED_TS         TIMESTAMP_NTZ
)
COMMENT = 'Rejected price snapshot source rows; raw source values are retained as VARCHAR';

-- ------------------------------------------------------------
-- 8. SILVER FILE-LEVEL CONTROL TABLE
-- This is an operational ledger, not the sole idempotency control.
-- Row-level MERGE on SRC_FILE_NM + ROW_NUM remains authoritative.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS SILVER_LOAD_CONTROL (
    CONTROL_ID          VARCHAR(36) NOT NULL,
    SOURCE_TABLE        VARCHAR(200) NOT NULL,
    TARGET_TABLE        VARCHAR(200) NOT NULL,
    SRC_FILE_NM         VARCHAR(1000) NOT NULL,
    SRC_FILE_SIZE_BYTES NUMBER(38,0),
    SRC_FILE_CHECKSUM   VARCHAR(128),
    BATCH_ID            VARCHAR(100),
    AIRFLOW_RUN_ID      VARCHAR(250),
    LOAD_STATUS         VARCHAR(20) NOT NULL,
    STARTED_TS          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_TS        TIMESTAMP_NTZ,
    ROWS_READ           NUMBER(38,0) DEFAULT 0,
    ROWS_LOADED         NUMBER(38,0) DEFAULT 0,
    ROWS_QUARANTINED    NUMBER(38,0) DEFAULT 0,
    ERROR_MESSAGE       VARCHAR(4000)
)
COMMENT = 'Silver processing status and row-count audit by source file and target table';

-- ------------------------------------------------------------
-- Quick object checks
-- ------------------------------------------------------------
SHOW TABLES IN SCHEMA RETAIL_DB.SILVER;

-- Expected rejection tables:
-- REJ_ITEM_MASTER
-- REJ_STORE_MASTER
-- REJ_CUSTOMER_MASTER
-- REJ_POS_SALES
-- REJ_INVENTORY_SNAPSHOT
-- REJ_PRICE_SNAPSHOT
