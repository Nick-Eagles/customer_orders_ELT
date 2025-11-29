/*
Truncate and populate silver tables

Warning: running this script deletes all rows in any existing silver tables!
Below are the queries I used to get from bronze to silver. For each bronze table
there is a silver equivalent (in name), maintaining a one-to-one mapping.

Due to the simplicity of the data sources, a single (sometimes slightly
monolithic) query is used to get from bronze to silver for each table, though
in a larger real-world project I would break up the logic into multiple silver
steps for clarity and maintainability.
*/

-- #############################################################################
-- #   CRM tables
-- #############################################################################

-- #----------------------------------------------------------------------------
-- #   silver.crm_cust_info
-- #----------------------------------------------------------------------------

TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
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
    FROM bronze.crm_cust_info
) AS sub
WHERE rn = 1 AND cst_id IS NOT NULL;

-- #----------------------------------------------------------------------------
-- #   silver.crm_prd_info
-- #----------------------------------------------------------------------------

TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info (
    prd_id,
    prd_key,
    prd_cat,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    sub.prd_id as prd_id,
    sub.prd_key as prd_key,
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
        -- Generally cast and clean up columns
        CAST(prd_id AS INT) AS prd_id,
        SUBSTRING(TRIM(prd_key) FROM 7) AS prd_key,
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
    FROM bronze.crm_prd_info
) sub;

-- #----------------------------------------------------------------------------
-- #   silver.crm_sales_details
-- #----------------------------------------------------------------------------

TRUNCATE TABLE silver.crm_sales_details;
INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
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
    FROM bronze.crm_sales_details
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
FROM cleaned;

-- #############################################################################
-- #   ERP tables
-- #############################################################################

-- #----------------------------------------------------------------------------
-- #   silver.erp_cust_az12
-- #----------------------------------------------------------------------------

TRUNCATE TABLE silver.erp_cust_az12;
INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT
    -- To match other tables, we care only about the integer part of the
    -- customer ID
    CAST(regexp_replace(cid, '^(NAS)*AW0*', '') AS INT) AS cid,
    TO_DATE(bdate, 'YYYY-MM-DD') AS bdate,
    CASE
        -- Clean up variations of male and female
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        -- Nulls or spaces become nulls
        WHEN gen IS NULL OR TRIM(gen) = '' THEN NULL
        ELSE TRIM(gen)
    END AS gen
FROM bronze.erp_cust_az12;

-- #----------------------------------------------------------------------------
-- #   silver.erp_loc_a101
-- #----------------------------------------------------------------------------

TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT
    -- To match other tables, we care only about the integer part of the
    -- customer ID
    CAST(regexp_replace(cid, '^AW-0*', '') AS INT) AS cid,
    CASE
        -- Clean up variations of country names
        WHEN UPPER(TRIM(cntry)) IN ('USA', 'UNITED STATES', 'US') THEN 'United States'
        WHEN UPPER(TRIM(cntry)) IN ('GERMANY', 'DE') THEN 'Germany'
        -- Nulls or spaces become nulls
        WHEN cntry IS NULL OR TRIM(cntry) = '' THEN NULL
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101;

-- #----------------------------------------------------------------------------
-- #   silver.erp_px_cat_g1v2
-- #----------------------------------------------------------------------------

-- This table is especially clean, and we can just use the bronze data. To keep
-- things consistent and organized though, we still create the silver table
TRUNCATE TABLE silver.erp_px_cat_g1v2;
INSERT INTO silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
SELECT * FROM bronze.erp_px_cat_g1v2;
