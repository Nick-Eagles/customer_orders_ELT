/*
Create gold views, including dimension and fact tables

Views are used to always have up-to-date data, and because performance is not
a concern for the scale of this project.
*/

-- #############################################################################
-- #  Dimension views
-- #############################################################################

-- All customer info joined together; one row per customer
DROP VIEW IF EXISTS gold.dim_customer;
CREATE VIEW gold.dim_customer AS
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
FROM silver.crm_cust_info AS crm
LEFT JOIN silver.erp_cust_az12 AS erp_az
    ON crm.cst_id = erp_az.cid_clean
LEFT JOIN silver.erp_loc_a101 AS erp_loc
    ON crm.cst_id = erp_loc.cid_clean;

-- All current product info joined with ERP category info; one row per product
DROP VIEW IF EXISTS gold.dim_product;
CREATE VIEW gold.dim_product AS
SELECT
    ROW_NUMBER() OVER (ORDER BY crm.prd_key) AS surrogate_key,
    -- Product IDs/ keys come in 3 forms; retain all of them
    crm.prd_id AS crm_key1,
    crm.prd_key AS crm_key2,
    crm.prd_clean_key AS crm_key3,
    -- Then put product info followed by category info
    crm.prd_nm AS name,
    crm.prd_cost AS cost,
    crm.prd_line AS line,
    crm.prd_start_dt AS start_date,
    crm.prd_cat AS category_key,
    erp.cat AS category_name,
    erp.subcat AS subcategory_name,
    erp.maintenance AS category_maintenance
FROM silver.crm_prd_info AS crm
LEFT JOIN silver.erp_px_cat_g1v2 AS erp
    ON crm.prd_cat = erp.id
-- For gold, only include products that are still active
WHERE crm.prd_end_dt IS NULL;
