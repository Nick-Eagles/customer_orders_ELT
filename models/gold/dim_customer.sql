{{ config(materialized='view') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY crm.cst_id) AS surrogate_key,
    -- Retain the 4 forms of customer keys seen in the raw data
    crm.cst_id AS crm_key_1,
    crm.cst_key AS crm_key_2,
    erp_az.cid AS erp_key_1,
    erp_loc.cid AS erp_key_2,
    crm.cst_firstname AS first_name,
    crm.cst_lastname AS last_name,
    crm.cst_marital_status AS marital_status,
    -- Prefer gender value from CRM where possible
    CASE
        WHEN crm.cst_gndr IS NULL THEN erp_az.gen
        WHEN crm.cst_gndr != erp_az.gen THEN NULL
        ELSE crm.cst_gndr
    END AS gender,
    crm.cst_create_date AS create_date,
    erp_az.bdate AS birth_date,
    erp_loc.cntry AS country
FROM {{ ref('crm_cust_info') }} AS crm
LEFT JOIN {{ ref('erp_cust_az12') }} AS erp_az
    ON crm.cst_id = erp_az.cid_clean
LEFT JOIN {{ ref('erp_loc_a101') }} AS erp_loc
    ON crm.cst_id = erp_loc.cid_clean
