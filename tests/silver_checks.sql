/*
Quality checks to test the silver-layer tables

The following queries include tests, generally expecting zero rows returned, to
ensure I've properly cleaned up the bronze data to get to silver
*/

-- #############################################################################
-- #   CRM tables
-- #############################################################################

-- #----------------------------------------------------------------------------
-- #   silver.crm_cust_info
-- #----------------------------------------------------------------------------

-- Primary key should neither be null nor duplicated
--
-- Expectation: 0 rows returned
SELECT cst_id, COUNT(*) AS n
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING cst_id IS NULL OR COUNT(*) > 1;

-- The Silver-layer code assumes certain values in the marital status and gender
-- columns (but preserves unexpected values where they exist). Check for
-- unexpected values
--
-- Expectation: 0 rows returned
SELECT * FROM silver.crm_cust_info
WHERE cst_marital_status NOT IN ('Married', 'Single')
   OR cst_gndr NOT IN ('Male', 'Female');

-- #----------------------------------------------------------------------------
-- #   silver.crm_prd_info
-- #----------------------------------------------------------------------------

-- I did some complex manipulation of dates to get to silver; just check that
-- end dates are valid here (not earlier than start dates)
--
-- Expectation: 0 rows returned
SELECT prd_start_dt, prd_end_dt
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- I technically allow, but don't expect product lines outside of this set
--
-- Expectation: 0 rows returned
SELECT * FROM silver.crm_prd_info
WHERE prd_line NOT IN ('Mountain', 'Road', 'Other sales', 'Touring');

-- Costs should be non-negative
--
-- Expectation: 0 rows returned
SELECT * FROM silver.crm_prd_info
WHERE prd_cost < 0;

-- Each product should have at most one row with a NULL end date
--
-- Expectation: 0 rows returned
SELECT prd_key, COUNT(*) AS null_end_date_count
FROM silver.crm_prd_info
WHERE prd_end_dt IS NULL
GROUP BY prd_key
HAVING COUNT(*) > 1;

-- #----------------------------------------------------------------------------
-- #   silver.crm_sales_details
-- #----------------------------------------------------------------------------

-- Quantities, prices, and sales amounts should be non-negative
--
-- Expectation: 0 rows returned
SELECT * FROM silver.crm_sales_details
WHERE sls_quantity < 0 OR sls_price < 0 OR sls_sales < 0;

-- All products should exist in the product info table
--
-- Expectation: 0 rows returned
SELECT DISTINCT sls_prd_key FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (
    SELECT prd_clean_key FROM silver.crm_prd_info
);

-- All customers should exist in the customer info table
--
-- Expectation: 0 rows returned
SELECT DISTINCT sls_cust_id FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (
    SELECT cst_id FROM silver.crm_cust_info
);

-- #############################################################################
-- #   ERP tables
-- #############################################################################

-- #----------------------------------------------------------------------------
-- #   silver.erp_cust_az12
-- #----------------------------------------------------------------------------

-- All variations of male and female should be the following values. Also, while
-- I allow for other gender values, I don't expect to see them in the data
--
-- Expectation: 0 rows returned
SELECT DISTINCT gen FROM silver.erp_cust_az12
WHERE gen NOT IN ('Male', 'Female');

-- Expect all cleaned customer IDs to exist in the CRM customer info table
--
-- Expectation: 0 rows returned
SELECT cid_clean FROM silver.erp_cust_az12
WHERE cid_clean NOT IN (
    SELECT cst_id FROM silver.crm_cust_info
);

-- #----------------------------------------------------------------------------
-- #   silver.erp_loc_a101
-- #----------------------------------------------------------------------------

-- Expect all customer IDs to exist in the CRM customer info table
--
-- Expectation: 0 rows returned
SELECT cid_clean FROM silver.erp_loc_a101
WHERE cid_clean NOT IN (
    SELECT cst_id FROM silver.crm_cust_info
);

-- Can't be sure of what countries might show up in the table in the future,
-- but certain variations of US and Germany should not exist
--
-- Expectation: 0 rows returned
SELECT DISTINCT cntry FROM silver.erp_loc_a101
WHERE UPPER(cntry) IN ('UNITED STATES OF AMERICA', 'USA', 'US', 'DEUTSCHLAND', 'DE');

-- #----------------------------------------------------------------------------
-- #   silver.erp_px_cat_g1v2
-- #----------------------------------------------------------------------------

-- Category IDs should obey the following regex
-- 
-- Expectation: 0 rows returned
SELECT id FROM silver.erp_px_cat_g1v2
WHERE id !~ '^[A-Z]{2}_[A-Z]{2}$';

-- Maintenance should be either Yes or No
--
-- Expectation: 0 rows returned
SELECT maintenance FROM silver.erp_px_cat_g1v2
WHERE maintenance NOT IN ('Yes', 'No');

-- How many products in the CRM product table don't have matching category info
-- in the ERP table? Relevant because I'll do a left join in the gold layer
--
-- This is not a hard check, but ideally should return < 0.1 (10%)
SELECT 
    AVG(
        CASE WHEN
            erp.id IS NULL AND
            erp.cat IS NULL AND
            erp.subcat IS NULL AND
            erp.maintenance IS NULL THEN 1
        ELSE 0
        END
    ) AS null_proportion
FROM silver.crm_prd_info AS crm
LEFT JOIN silver.erp_px_cat_g1v2 AS erp
    ON crm.prd_cat = erp.id;
