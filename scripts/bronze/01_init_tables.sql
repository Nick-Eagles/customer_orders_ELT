/*
Create Bronze-layer tables

Warning: this script will drop any existing Bronze tables! I cautiously use
VARCHAR(50) in many cases to avoid dropping poorly formatted data during
ingestion; the priority is preserving the raw data as-is.
*/

-- #############################################################################
-- #  Create Bronze tables from CRM source
-- #############################################################################

DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id VARCHAR(50),
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost VARCHAR(50),
    prd_line VARCHAR(50),
    prd_start_dt VARCHAR(50),
    prd_end_dt VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id VARCHAR(50),
    sls_order_dt VARCHAR(50),
    sls_ship_dt VARCHAR(50),
    sls_due_dt VARCHAR(50),
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

-- #############################################################################
-- #  Create Bronze tables from ERP source
-- #############################################################################

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    CID VARCHAR(50),
    BDATE VARCHAR(50),
    GEN VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    CID VARCHAR(50),
    CNTRY VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    ID VARCHAR(50),
    CAT VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(50)
);
