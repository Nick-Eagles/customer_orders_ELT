-- Table: silver erp_loc_a101
--
-- Test: check for country names that should've been cleaned out in the silver
-- layer
SELECT DISTINCT cntry FROM {{ ref('erp_loc_a101') }}
WHERE UPPER(cntry) IN ('UNITED STATES OF AMERICA', 'USA', 'US', 'DEUTSCHLAND', 'DE')
