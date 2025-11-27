/*
Bulk insert data into Bronze-layer tables from CSV files

After 01_init_tables.sql, this script truncates any existing data, then loads
fresh data from the source CSVs (using absolute paths on the local filesystem
in this case for compatibility with DBeaver).
*/

-- #############################################################################
-- #  Load CRM source data
-- #############################################################################

TRUNCATE TABLE bronze.crm_cust_info;
COPY bronze.crm_cust_info
FROM '/var/lib/postgresql/imports/cust_info.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

TRUNCATE TABLE bronze.crm_prd_info;
COPY bronze.crm_prd_info
FROM '/var/lib/postgresql/imports/prd_info.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

TRUNCATE TABLE bronze.crm_sales_details;
COPY bronze.crm_sales_details
FROM '/var/lib/postgresql/imports/sales_details.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

-- #############################################################################
-- #  Load ERP source data
-- #############################################################################

TRUNCATE TABLE bronze.erp_cust_az12;
COPY bronze.erp_cust_az12
FROM '/var/lib/postgresql/imports/CUST_AZ12.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

TRUNCATE TABLE bronze.erp_loc_a101;
COPY bronze.erp_loc_a101
FROM '/var/lib/postgresql/imports/LOC_A101.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
COPY bronze.erp_px_cat_g1v2
FROM '/var/lib/postgresql/imports/PX_CAT_G1V2.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');
