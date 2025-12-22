-- Table: silver crm_prd_info
--
-- Test: a product should have at most one row with a NULL end date, because
-- later we use this to indicate whether a product is still active
SELECT prd_key, COUNT(*) AS null_end_date_count
FROM {{ ref('crm_prd_info') }}
WHERE prd_end_dt IS NULL
GROUP BY prd_key
HAVING COUNT(*) > 1
