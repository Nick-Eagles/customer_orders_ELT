/*
Quality checks to test the gold-layer views

The following queries include tests, generally expecting zero rows returned, to
ensure proper content of the gold views
*/

-- #############################################################################
-- #   Dimension views
-- #############################################################################

-- I'm left joining in two tables with silver.crm_cust_info and expect the
-- customer key from each row to exist in those two tables. Otherwise, there
-- would be NULLs that get detected here
--
-- Expectation: 0 rows returned
SELECT * FROM gold.dim_customer
WHERE erp_key_1 IS NULL OR erp_key_2 IS NULL;

-- By gold we should have one row per product, a specification not met by the
-- silver layer
--
-- Expectation: 0 rows returned
SELECT crm_key_2, COUNT(*) AS n
FROM gold.dim_product
GROUP BY crm_key_2
HAVING COUNT(*) > 1;

-- #############################################################################
-- #   Fact views
-- #############################################################################

-- Every customer and product from a sale should exist in the corresponding
-- dimension table
--
-- Expectation: 0 rows returned
SELECT * FROM gold.fact_sales
WHERE customer_key IS NULL OR product_key IS NULL;
