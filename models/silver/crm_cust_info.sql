{{ config(materialized='table') }}

SELECT
    CAST(cst_id AS INT) AS cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname)  AS cst_lastname,
    -- Use full names instead of abbreviations but preseve null or unexpected
    -- values
    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN cst_marital_status IS NULL THEN NULL
        ELSE cst_marital_status
    END AS cst_marital_status,
    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN cst_gndr IS NULL THEN NULL
        ELSE cst_gndr
    END AS cst_gndr,
    TO_DATE(cst_create_date, 'YYYY-MM-DD') AS cst_create_date
-- Take the latest entry for customers with multiple records
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id
            ORDER BY TO_DATE(cst_create_date, 'YYYY-MM-DD') DESC
        ) AS rn
    FROM {{ source('bronze', 'crm_cust_info') }}
) AS sub
WHERE rn = 1 AND cst_id IS NOT NULL
