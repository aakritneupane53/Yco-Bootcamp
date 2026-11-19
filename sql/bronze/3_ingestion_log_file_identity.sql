-- Add file identity columns to the existing Bronze ingestion log.
-- The DAG also runs these ALTER statements idempotently at runtime.
ALTER TABLE RETAIL_DB.STG.STG_INGESTION_LOG
    ADD COLUMN IF NOT EXISTS FILE_CHECKSUM VARCHAR(64);

ALTER TABLE RETAIL_DB.STG.STG_INGESTION_LOG
    ADD COLUMN IF NOT EXISTS FILE_SIZE_BYTES NUMBER(38,0);
