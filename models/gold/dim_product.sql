SELECT
    ROW_NUMBER() OVER (ORDER BY crm.prd_key) AS surrogate_key,
    -- Product IDs/ keys come in 3 forms; retain all of them
    crm.prd_id AS crm_key_1,
    crm.prd_key AS crm_key_2,
    crm.prd_clean_key AS crm_key_3,
    -- Then put product info followed by category info
    crm.prd_nm AS name,
    crm.prd_cost AS cost,
    crm.prd_line AS line,
    crm.prd_start_dt AS start_date,
    crm.prd_cat AS category_key,
    erp.cat AS category_name,
    erp.subcat AS subcategory_name,
    erp.maintenance AS category_maintenance
FROM {{ ref('crm_prd_info') }} AS crm
LEFT JOIN {{ ref('erp_px_cat_g1v2') }} AS erp
    ON crm.prd_cat = erp.id
-- This trick keeps the latest version of a product by prefering a NULL end
-- date, or the latest end date if no NULL exists
WHERE (crm.prd_key, COALESCE(crm.prd_end_dt, '9999-12-31')) IN (
    SELECT prd_key, MAX(COALESCE(prd_end_dt, '9999-12-31'))
    FROM {{ ref('crm_prd_info') }}
    GROUP BY prd_key
)
