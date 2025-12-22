-- Table: silver crm_prd_info
--
-- Test: costs should be non-negative
SELECT * FROM {{ ref('crm_prd_info') }}
WHERE prd_cost < 0
