SELECT
    cid,
    -- To match other tables, we care only about the integer part of the
    -- customer ID
    CAST(regexp_replace(cid, '^(NAS)*AW0*', '') AS INT) AS cid_clean,
    TO_DATE(bdate, 'YYYY-MM-DD') AS bdate,
    CASE
        -- Clean up variations of male and female
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        -- Nulls or spaces become nulls
        WHEN gen IS NULL OR TRIM(gen) = '' THEN NULL
        ELSE TRIM(gen)
    END AS gen
FROM {{ source('bronze', 'erp_cust_az12') }}
