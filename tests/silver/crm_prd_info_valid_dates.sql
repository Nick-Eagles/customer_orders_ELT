-- Table: silver crm_prd_info
--
-- Test: end dates should always be later than or equal to start dates
SELECT prd_start_dt, prd_end_dt
FROM {{ ref('crm_prd_info') }}
WHERE prd_end_dt < prd_start_dt
