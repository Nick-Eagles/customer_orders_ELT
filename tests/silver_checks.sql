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
