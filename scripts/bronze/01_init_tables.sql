/*
Create Bronze-layer tables

Warning: this script will drop any existing Bronze tables! I cautiously use
TEXT in many cases to avoid dropping poorly formatted data during
ingestion; the priority is preserving the raw data as-is.
*/

-- #############################################################################
-- #  Create Bronze tables from CRM source
-- #############################################################################

DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id TEXT,
    cst_key TEXT,
    cst_firstname TEXT,
    cst_lastname TEXT,
    cst_marital_status TEXT,
    cst_gndr TEXT,
    cst_create_date TEXT
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id TEXT,
    prd_key TEXT,
    prd_nm TEXT,
    prd_cost TEXT,
    prd_line TEXT,
    prd_start_dt TEXT,
    prd_end_dt TEXT
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num TEXT,
    sls_prd_key TEXT,
    sls_cust_id TEXT,
    sls_order_dt TEXT,
    sls_ship_dt TEXT,
    sls_due_dt TEXT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

-- #############################################################################
-- #  Create Bronze tables from ERP source
-- #############################################################################

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    CID TEXT,
    BDATE TEXT,
    GEN TEXT
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    CID TEXT,
    CNTRY TEXT
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    ID TEXT,
    CAT TEXT,
    SUBCAT TEXT,
    MAINTENANCE TEXT
);
