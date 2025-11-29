/*
Create silver-layer tables

Warning: this script will drop any existing silver tables!
*/

-- #############################################################################
-- #  Create silver tables from CRM source
-- #############################################################################

DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key TEXT,
    cst_firstname TEXT,
    cst_lastname TEXT,
    cst_marital_status TEXT,
    cst_gndr TEXT,
    cst_create_date DATE
);

DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    prd_key TEXT,
    prd_cat CHAR(5),
    prd_nm TEXT,
    prd_cost INT,
    prd_line TEXT,
    prd_start_dt DATE,
    prd_end_dt DATE
);

DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num TEXT,
    sls_prd_key TEXT,
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

-- #############################################################################
-- #  Create silver tables from ERP source
-- #############################################################################

DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    CID INT,
    BDATE DATE,
    GEN TEXT
);

DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    CID INT,
    CNTRY TEXT
);

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    ID CHAR(5),
    CAT TEXT,
    SUBCAT TEXT,
    MAINTENANCE TEXT
);
