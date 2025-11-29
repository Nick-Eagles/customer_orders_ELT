-- #############################################################################
-- #   CRM tables
-- #############################################################################

-- #----------------------------------------------------------------------------
-- #   bronze.crm_cust_info
-- #----------------------------------------------------------------------------

-- I want to use 'cust_id' as the key for customers, since it best matches
-- other tables. Here I check what's going on with duplicate keys; it turns out
-- in rare cases customers have multiple records with different dates, and in
-- all such cases the latest date provide the most information.
SELECT t.*
FROM bronze.crm_cust_info AS t
JOIN (
    SELECT cst_id
    FROM bronze.crm_cust_info
    GROUP BY cst_id
    HAVING COUNT(*) > 1
) AS dup
    ON t.cst_id = dup.cst_id;

-- To be extra cautious about invalid date entries, I just checked if non-null
-- entries could be cast to dates; there were no such invalid entries.
SELECT * FROM bronze.crm_cust_info
WHERE cst_create_date !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';

-- #----------------------------------------------------------------------------
-- #   bronze.crm_prd_info
-- #----------------------------------------------------------------------------

-- Initially it looked like 'prd_key' was the key, but a single product can have
-- multiple costs over time. This query revealed however that end dates were
-- often invalid (earlier than start dates)
SELECT t.*
FROM bronze.crm_prd_info AS t
JOIN (
    SELECT prd_key
    FROM bronze.crm_prd_info
    GROUP BY prd_key
    HAVING COUNT(*) > 1
) AS dup
    ON t.prd_key = dup.prd_key;

-- More sanity checks that non-NULL dates are properly formatted
SELECT * FROM bronze.crm_prd_info
WHERE prd_start_dt !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    OR prd_end_dt !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';

-- A huge fraction of rows have invalid end dates (earlier than start dates)
SELECT
100 * AVG(
    CASE WHEN TO_DATE(prd_end_dt, 'YYYY-MM-DD') < TO_DATE(prd_start_dt, 'YYYY-MM-DD') THEN 1 ELSE 0 END
) FROM bronze.crm_prd_info;

-- Just making sure non-null product costs are non-negative integers (they are)
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE (prd_cost IS NOT NULL AND CAST(prd_cost AS INT) IS NULL)
    OR CAST(prd_cost AS INT) < 0;

-- Lining up products across tables, I noticed one table essentially includes a
-- substring of the other
SELECT DISTINCT sls_prd_key FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (
    SELECT substring(prd_key FROM 7) FROM bronze.crm_prd_info
);

-- #----------------------------------------------------------------------------
-- #   bronze.crm_sales_details
-- #----------------------------------------------------------------------------

-- There are several cases of negative columns (that can't be negative).
-- Moreover, I see that sales is not always equal to quantity * price
SELECT sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details
WHERE sls_sales < 0 OR sls_quantity < 0 OR sls_price < 0;

-- Follow up on the last observation. It looks like in general it's not possible
-- to correct for bad values, but NULL values in one column can be recovered
SELECT sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details
WHERE ABS(sls_sales) != ABS(sls_quantity) * ABS(sls_price) OR
sls_sales is NULL OR sls_quantity IS NULL OR sls_price IS NULL;

-- Some dates are simply invalid
SELECT sls_order_dt, sls_ship_dt, sls_due_dt
FROM bronze.crm_sales_details
WHERE CHAR_LENGTH(sls_order_dt) != 8 OR
    CHAR_LENGTH(sls_ship_dt) != 8 OR
    CHAR_LENGTH(sls_due_dt) != 8;

-- There are no nonsensical ordering of date columns
SELECT sls_order_dt, sls_ship_dt, sls_due_dt
FROM (
    SELECT * FROM bronze.crm_sales_details
    WHERE CHAR_LENGTH(sls_order_dt) = 8 AND
        CHAR_LENGTH(sls_ship_dt) = 8 AND
        CHAR_LENGTH(sls_due_dt) = 8
)
WHERE TO_DATE(sls_order_dt, 'YYYYMMDD') > TO_DATE(sls_ship_dt, 'YYYYMMDD')
    OR TO_DATE(sls_ship_dt, 'YYYYMMDD') > TO_DATE(sls_due_dt, 'YYYYMMDD');

-- #############################################################################
-- #   ERP tables
-- #############################################################################

-- #----------------------------------------------------------------------------
-- #   bronze.erp_cust_az12
-- #----------------------------------------------------------------------------

-- Check if dates are reasonably formatted
SELECT bdate FROM bronze.erp_cust_az12 
WHERE bdate !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';

-- Check what genders are present
SELECT DISTINCT gen FROM bronze.erp_cust_az12;

-- Figure out how customer IDs in this table line up with those in CRM
SELECT CAST(regexp_replace(cid, '^(NAS)*AW0*', '') AS INT)
FROM bronze.erp_cust_az12
WHERE CAST(regexp_replace(cid, '^(NAS)*AW0*', '') AS INT) NOT IN (
    SELECT cst_id FROM silver.crm_cust_info
);

-- #----------------------------------------------------------------------------
-- #   bronze.erp_loc_a101
-- #----------------------------------------------------------------------------

-- Check how customer IDs like up with the main CRM customer table
SELECT CAST(regexp_replace(cid, '^AW-0*', '') AS INT)
FROM bronze.erp_loc_a101
WHERE CAST(regexp_replace(cid, '^AW-0*', '') AS INT) NOT IN (
    SELECT cst_id FROM silver.crm_cust_info
);

-- I see there are entries with just spaces as well as multiple names for
-- United States and Germany
SELECT DISTINCT cntry FROM bronze.erp_loc_a101;
