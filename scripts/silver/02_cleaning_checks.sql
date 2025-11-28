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
SELECT *
FROM bronze.crm_cust_info
WHERE cst_create_date IS NOT NULL
    AND TO_DATE(cst_create_date, 'YYYY-MM-DD') IS NULL;

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
SELECT *
FROM bronze.crm_prd_info
WHERE (
    prd_start_dt IS NOT NULL
    AND TO_DATE(prd_start_dt, 'YYYY-MM-DD') IS NULL
    ) OR (
    prd_end_dt IS NOT NULL
    AND TO_DATE(prd_end_dt, 'YYYY-MM-DD') IS NULL
);

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
