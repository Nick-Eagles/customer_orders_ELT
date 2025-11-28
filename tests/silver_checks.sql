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
