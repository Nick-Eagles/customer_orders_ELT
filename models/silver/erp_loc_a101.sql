{{ config(materialized='table') }}

SELECT
    cid,
    -- To match other tables, we care only about the integer part of the
    -- customer ID
    CAST(regexp_replace(cid, '^AW-0*', '') AS INT) AS cid_clean,
    CASE
        -- Clean up variations of country names
        WHEN UPPER(TRIM(cntry)) IN ('USA', 'UNITED STATES', 'US') THEN 'United States'
        WHEN UPPER(TRIM(cntry)) IN ('GERMANY', 'DE') THEN 'Germany'
        -- Nulls or spaces become nulls
        WHEN cntry IS NULL OR TRIM(cntry) = '' THEN NULL
        ELSE TRIM(cntry)
    END AS cntry
FROM {{ source('bronze', 'erp_loc_a101') }}
