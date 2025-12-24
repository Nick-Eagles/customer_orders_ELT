SELECT
    sub.prd_id as prd_id,
    sub.prd_key as prd_key,
    sub.prd_clean_key AS prd_clean_key,
    sub.prd_cat AS prd_cat,
    sub.prd_nm as prd_nm,
    sub.prd_cost as prd_cost,
    sub.prd_line as prd_line,
    CASE
        -- Swap start and end for invalid end dates if there aren't multiple
        -- records for this product
        WHEN sub.group_size = 1 AND sub.prd_end < sub.prd_start THEN sub.prd_end
        ELSE sub.prd_start
    END AS prd_start_dt,
    CASE
        -- Whenever a product has multiple records, the end date for a given
        -- record must be just before the next start date
        WHEN sub.next_start IS NOT NULL THEN CAST(sub.next_start - INTERVAL '1 day' AS DATE)
        -- Swap start and end for invalid end dates (except when the last of
        -- multiple records has a bad end date, in which case we drop the end
        -- date)
        WHEN sub.group_size = 1 AND sub.prd_end < sub.prd_start THEN sub.prd_start
        WHEN sub.prd_end < sub.prd_start THEN NULL
        ELSE sub.prd_end
    END AS prd_end_dt
FROM (
    SELECT
        -- Generally cast and clean up columns. Preserve original prd_key but
        -- derive a cleaner version that joins with other tables
        CAST(prd_id AS INT) AS prd_id,
        prd_key,
        SUBSTRING(TRIM(prd_key) FROM 7) AS prd_clean_key,
        regexp_replace(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_') AS prd_cat,
        TRIM(prd_nm) AS prd_nm,
        CAST(prd_cost AS INT) AS prd_cost,
        -- Use full names instead of abbreviations but preseve null or unexpected
        -- values
        CASE
            WHEN TRIM(prd_line) = 'M' THEN 'Mountain'
            WHEN TRIM(prd_line) = 'R' THEN 'Road'
            WHEN TRIM(prd_line) = 'S' THEN 'Other sales'
            WHEN TRIM(prd_line) = 'T' THEN 'Touring'
            WHEN TRIM(prd_line) IS NULL THEN NULL
            ELSE TRIM(prd_line)
        END AS prd_line,
        TO_DATE(prd_start_dt, 'YYYY-MM-DD') AS prd_start,
        TO_DATE(prd_end_dt, 'YYYY-MM-DD')   AS prd_end,
        -- Check the next start date if one exists
        LEAD(TO_DATE(prd_start_dt, 'YYYY-MM-DD'))
            OVER (PARTITION BY prd_key ORDER BY TO_DATE(prd_start_dt, 'YYYY-MM-DD')) AS next_start,
        -- Number of records per product
        COUNT(*) OVER (PARTITION BY prd_key) AS group_size
    FROM {{ source('bronze', 'crm_prd_info') }} 
) sub
