-- Table: silver crm_sales_details
--
-- Test: within an order, a customer shouldn't order a product in more than one
-- row; instead this would be reflected conceptually with sls_quantity > 1
SELECT * FROM {{ ref('crm_sales_details') }}
WHERE sls_ord_num IN (
    SELECT sls_ord_num
    FROM {{ ref('crm_sales_details') }}
    GROUP BY sls_ord_num, sls_cust_id, sls_prd_key
    HAVING COUNT(*) > 1
)
