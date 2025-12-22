{{ config(materialized='table') }}

WITH cleaned AS (
    SELECT
        TRIM(sls_ord_num) AS sls_ord_num,
        TRIM(sls_prd_key) AS sls_prd_key,
        CAST(sls_cust_id AS INT) AS sls_cust_id,
        -- This particular column has routine issues with invalid dates
        CASE
            WHEN CHAR_LENGTH(sls_order_dt) = 8 THEN TO_DATE(sls_order_dt, 'YYYYMMDD')
            ELSE NULL
        END AS sls_order_dt,
        TO_DATE(sls_ship_dt, 'YYYYMMDD') AS sls_ship_dt,
        TO_DATE(sls_due_dt, 'YYYYMMDD') AS sls_due_dt,
        ABS(sls_sales) AS sls_sales,
        ABS(sls_quantity) AS sls_quantity,
        ABS(sls_price) AS sls_price,
        -- If there is up to one null among sale columns, a null can be imputed
        (
            (CASE WHEN sls_sales IS NULL THEN 1 ELSE 0 END) +
            (CASE WHEN sls_quantity IS NULL THEN 1 ELSE 0 END) +
            (CASE WHEN sls_price IS NULL THEN 1 ELSE 0 END)
        ) <= 1 AS sales_are_well_behaved
    FROM {{ source('bronze', 'crm_sales_details') }}
) SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    -- Impute missing sales/quantity/price values where possible. Note I'm
    -- expecting integer results, and just retain nulls otherwise
    CASE
        WHEN sls_sales IS NULL AND sales_are_well_behaved THEN sls_quantity * sls_price
        ELSE sls_sales
    END AS sls_sales,
    CASE
        WHEN sls_quantity IS NULL AND sales_are_well_behaved AND (sls_sales % sls_price = 0) THEN
            CAST(sls_sales / sls_price AS INT)
        ELSE sls_quantity
    END AS sls_quantity,
    CASE
        WHEN sls_price IS NULL AND sales_are_well_behaved AND (sls_sales % sls_quantity = 0) THEN
            CAST(sls_sales / sls_quantity AS INT)
        ELSE sls_price
    END AS sls_price
FROM cleaned
