
-- DIM_CATEGORY (SCD1).
-- Retain category business keys found across ALL item snapshots.
-- Use the latest available name and derive current status from
-- the latest snapshot of each item.

MERGE INTO RETAIL_DB.TGT.DIM_CATEGORY T
USING (
    WITH ALL_ROWS AS (
        SELECT
            CATEGORY_ID,
            CATEGORY_NAME,
            SRC_SYS_C,
            BATCH_ID,
            SNAPSHOT_DT,
            UPDATE_TS,
            SRC_FILE_NM,
            ROW_NUM
        FROM RETAIL_DB.TMP.SILVER_ITEM_MASTER
        WHERE CATEGORY_ID IS NOT NULL
    ),

    LATEST_CATEGORY AS (
        SELECT
            CATEGORY_ID,
            CATEGORY_NAME,
            SRC_SYS_C,
            BATCH_ID
        FROM ALL_ROWS
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CATEGORY_ID
            ORDER BY SNAPSHOT_DT DESC,
                     UPDATE_TS DESC,
                     SRC_FILE_NM DESC,
                     ROW_NUM DESC
        ) = 1
    ),

    LATEST_ITEM AS (
        SELECT
            ITEM_ID,
            CATEGORY_ID,
            ACTIVE_FLG
        FROM RETAIL_DB.TMP.SILVER_ITEM_MASTER
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ITEM_ID
            ORDER BY SNAPSHOT_DT DESC,
                     UPDATE_TS DESC,
                     SRC_FILE_NM DESC,
                     ROW_NUM DESC
        ) = 1
    ),

    CATEGORY_STATUS AS (
        SELECT
            CATEGORY_ID,
            BOOLOR_AGG(COALESCE(ACTIVE_FLG, FALSE)) AS ACTIVE_FLG
        FROM LATEST_ITEM
        WHERE CATEGORY_ID IS NOT NULL
        GROUP BY CATEGORY_ID
    )

    SELECT
        N.CATEGORY_ID,
        N.CATEGORY_NAME AS CATEGORY_NM,
        COALESCE(S.ACTIVE_FLG, FALSE) AS ACTIVE_FLG,
        N.SRC_SYS_C,
        N.BATCH_ID
    FROM LATEST_CATEGORY N
    LEFT JOIN CATEGORY_STATUS S
        ON S.CATEGORY_ID = N.CATEGORY_ID

) S
ON T.CATEGORY_ID = S.CATEGORY_ID

WHEN MATCHED THEN UPDATE SET
    T.CATEGORY_NM = S.CATEGORY_NM,
    T.ACTIVE_FLG = S.ACTIVE_FLG,
    T.SRC_SYS_C = S.SRC_SYS_C,
    T.BATCH_ID = S.BATCH_ID,
    T.UPDATE_TS = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    CATEGORY_ID,
    CATEGORY_NM,
    ACTIVE_FLG,
    SRC_SYS_C,
    BATCH_ID,
    INSERT_TS,
    UPDATE_TS
)
VALUES (
    S.CATEGORY_ID,
    S.CATEGORY_NM,
    S.ACTIVE_FLG,
    S.SRC_SYS_C,
    S.BATCH_ID,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);
