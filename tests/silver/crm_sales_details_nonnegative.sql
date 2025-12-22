-- Table: silver crm_sales_details
--
-- Quantities, prices, and sales amounts should be non-negative
SELECT * FROM {{ ref('crm_sales_details') }}
WHERE sls_quantity < 0 OR sls_price < 0 OR sls_sales < 0
