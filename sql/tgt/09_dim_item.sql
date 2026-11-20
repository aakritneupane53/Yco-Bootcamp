
-- DIM_ITEM (SCD2).
-- Tracked attributes: ITEM_NM, SUBCATEGORY_KY, BRAND_KY and ACTIVE_FLG.
--
-- Historical baseline assumption:
-- The first observed item snapshot may be later than the item's first
-- recorded business activity. In that case, use the earliest activity
-- date from sales, inventory or price as the initial EFF_FROM_DT.
-- Subsequent SCD2 versions retain their actual SNAPSHOT_DT.
--
-- UNIT_OF_MEASURE remains in TMP and is excluded from the TGT design.

MERGE INTO RETAIL_DB.TGT.DIM_ITEM T
USING (
    WITH ITEM_ACTIVITY AS (
        SELECT
            ITEM_ID,
            MIN(BUSINESS_DATE) AS FIRST_ACTIVITY_DT
        FROM (
            SELECT ITEM_ID, BUSINESS_DATE
            FROM RETAIL_DB.TMP.SILVER_POS_SALES

            UNION ALL

            SELECT ITEM_ID, BUSINESS_DATE
            FROM RETAIL_DB.TMP.SILVER_INVENTORY_SNAPSHOT

            UNION ALL

            SELECT ITEM_ID, BUSINESS_DATE
            FROM RETAIL_DB.TMP.SILVER_PRICE_SNAPSHOT
        ) A
        WHERE ITEM_ID IS NOT NULL
          AND BUSINESS_DATE IS NOT NULL
        GROUP BY ITEM_ID
    ),

    BASE AS (
        SELECT
            S.ITEM_ID,
            S.ITEM_NAME AS ITEM_NM,
            SC.SUBCATEGORY_KY,
            B.BRAND_KY,
            S.ACTIVE_FLG,
            S.SNAPSHOT_DT,
            A.FIRST_ACTIVITY_DT,
            S.SRC_SYS_C,
            S.BATCH_ID
        FROM RETAIL_DB.TMP.SILVER_ITEM_MASTER S

        JOIN RETAIL_DB.TGT.DIM_CATEGORY C
            ON C.CATEGORY_ID = S.CATEGORY_ID

        JOIN RETAIL_DB.TGT.DIM_SUBCATEGORY SC
            ON SC.CATEGORY_KY = C.CATEGORY_KY
           AND SC.SUBCATEGORY_ID = S.SUBCATEGORY_ID

        LEFT JOIN RETAIL_DB.TGT.DIM_BRAND B
            ON B.BRAND_ID = S.BRAND_ID

        LEFT JOIN ITEM_ACTIVITY A
            ON A.ITEM_ID = S.ITEM_ID

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY S.ITEM_ID, S.SNAPSHOT_DT
            ORDER BY
                S.UPDATE_TS DESC,
                S.SRC_FILE_NM DESC,
                S.ROW_NUM DESC
        ) = 1
    ),

    COMPARED AS (
        SELECT
            *,
            LAG(SNAPSHOT_DT) OVER (
                PARTITION BY ITEM_ID
                ORDER BY SNAPSHOT_DT
            ) AS PREV_SNAPSHOT_DT,

            LAG(ITEM_NM) OVER (
                PARTITION BY ITEM_ID
                ORDER BY SNAPSHOT_DT
            ) AS PREV_ITEM_NM,

            LAG(SUBCATEGORY_KY) OVER (
                PARTITION BY ITEM_ID
                ORDER BY SNAPSHOT_DT
            ) AS PREV_SUBCATEGORY_KY,

            LAG(BRAND_KY) OVER (
                PARTITION BY ITEM_ID
                ORDER BY SNAPSHOT_DT
            ) AS PREV_BRAND_KY,

            LAG(ACTIVE_FLG) OVER (
                PARTITION BY ITEM_ID
                ORDER BY SNAPSHOT_DT
            ) AS PREV_ACTIVE_FLG

        FROM BASE
    ),

    CHANGES AS (
        SELECT *
        FROM COMPARED
        WHERE PREV_SNAPSHOT_DT IS NULL
           OR COALESCE(ITEM_NM, '') <> COALESCE(PREV_ITEM_NM, '')
           OR COALESCE(SUBCATEGORY_KY, -1)
                <> COALESCE(PREV_SUBCATEGORY_KY, -1)
           OR COALESCE(BRAND_KY, -1)
                <> COALESCE(PREV_BRAND_KY, -1)
           OR COALESCE(ACTIVE_FLG, FALSE)
                <> COALESCE(PREV_ACTIVE_FLG, FALSE)
    ),

    VERSIONED AS (
        SELECT
            *,
            LEAD(SNAPSHOT_DT) OVER (
                PARTITION BY ITEM_ID
                ORDER BY SNAPSHOT_DT
            ) AS NEXT_EFF_FROM_DT,

            ROW_NUMBER() OVER (
                PARTITION BY ITEM_ID
                ORDER BY SNAPSHOT_DT
            ) AS VERSION_NBR

        FROM CHANGES
    ),

    SOURCE_VERSIONS AS (
        SELECT
            ITEM_ID,
            ITEM_NM,
            SUBCATEGORY_KY,
            BRAND_KY,
            ACTIVE_FLG,
            SNAPSHOT_DT,
            VERSION_NBR,

            CASE
                WHEN VERSION_NBR = 1
                    THEN LEAST(
                        SNAPSHOT_DT,
                        COALESCE(FIRST_ACTIVITY_DT, SNAPSHOT_DT)
                    )
                ELSE SNAPSHOT_DT
            END AS EFF_FROM_DT,

            COALESCE(
                DATEADD(DAY, -1, NEXT_EFF_FROM_DT),
                DATE '9999-12-31'
            ) AS EFF_TO_DT,

            IFF(
                NEXT_EFF_FROM_DT IS NULL,
                TRUE,
                FALSE
            ) AS CURR_FLG,

            SRC_SYS_C,
            BATCH_ID

        FROM VERSIONED
    )

    SELECT *
    FROM SOURCE_VERSIONS

) S

-- Match the initial version using either its corrected effective date
-- or its original snapshot date. This makes the correction idempotent
-- and updates the existing surrogate-key row rather than inserting
-- an unnecessary replacement row.
ON T.ITEM_ID = S.ITEM_ID
AND (
    T.EFF_FROM_DT = S.EFF_FROM_DT
    OR (
        S.VERSION_NBR = 1
        AND T.EFF_FROM_DT = S.SNAPSHOT_DT
    )
)

WHEN MATCHED THEN UPDATE SET
    T.ITEM_NM = S.ITEM_NM,
    T.SUBCATEGORY_KY = S.SUBCATEGORY_KY,
    T.BRAND_KY = S.BRAND_KY,
    T.ACTIVE_FLG = S.ACTIVE_FLG,
    T.EFF_FROM_DT = S.EFF_FROM_DT,
    T.EFF_TO_DT = S.EFF_TO_DT,
    T.CURR_FLG = S.CURR_FLG,
    T.SRC_SYS_C = S.SRC_SYS_C,
    T.BATCH_ID = S.BATCH_ID,
    T.UPDATE_TS = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    ITEM_ID,
    ITEM_NM,
    SUBCATEGORY_KY,
    BRAND_KY,
    ACTIVE_FLG,
    CURR_FLG,
    EFF_FROM_DT,
    EFF_TO_DT,
    SRC_SYS_C,
    BATCH_ID,
    INSERT_TS,
    UPDATE_TS
)
VALUES (
    S.ITEM_ID,
    S.ITEM_NM,
    S.SUBCATEGORY_KY,
    S.BRAND_KY,
    S.ACTIVE_FLG,
    S.CURR_FLG,
    S.EFF_FROM_DT,
    S.EFF_TO_DT,
    S.SRC_SYS_C,
    S.BATCH_ID,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);
