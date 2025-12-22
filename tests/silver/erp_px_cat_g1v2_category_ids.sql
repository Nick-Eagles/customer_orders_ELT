-- Table: silver erp_px_cat_g1v2
-- 
-- Test: category IDs should match a specific regex pattern
SELECT id FROM {{ ref('erp_px_cat_g1v2') }}
WHERE id !~ '^[A-Z]{2}_[A-Z]{2}$'
