-- Table: silver erp_px_cat_g1v2
--
-- Test: rather than a hard test here, I'm really checking if a metric exceeds
-- a threshold, and throwing a warning if it does. Specifically, since a left
-- join will be done in the gold layer, I check how many products in the CRM
-- product table don't have matching category info in the ERP table. Warn if
-- this exceeds 10%

{{ config(severity='warn') }}

SELECT 
    AVG(
        CASE WHEN
            erp.id IS NULL AND
            erp.cat IS NULL AND
            erp.subcat IS NULL AND
            erp.maintenance IS NULL THEN 1
        ELSE 0
        END
    ) AS null_proportion
FROM {{ ref('crm_prd_info') }} AS crm
LEFT JOIN {{ ref('erp_px_cat_g1v2') }} AS erp
    ON crm.prd_cat = erp.id
