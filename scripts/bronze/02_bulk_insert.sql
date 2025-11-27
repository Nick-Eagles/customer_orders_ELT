/*
Bulk insert data into Bronze-layer tables from CSV files

After 01_init_tables.sql, this script truncates any existing data, then loads
fresh data from the source CSVs (using absolute paths on the local filesystem
in this case).
*/

-- #############################################################################
-- #  Load CRM source data
-- #############################################################################

TRUNCATE TABLE bronze.crm_cust_info;
COPY bronze.crm_cust_info
FROM '/home/nick/Documents/programming/customer_orders_ELT/data/source_CRM/cust_info.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', FREEZE true);

TRUNCATE TABLE bronze.crm_prd_info;
COPY bronze.crm_prd_info
FROM '/home/nick/Documents/programming/customer_orders_ELT/data/source_CRM/prd_info.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', FREEZE true);

TRUNCATE TABLE bronze.crm_sales_details;
COPY bronze.crm_sales_details
FROM '/home/nick/Documents/programming/customer_orders_ELT/data/source_CRM/sales_details.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', FREEZE true);

-- #############################################################################
-- #  Load ERP source data
-- #############################################################################

TRUNCATE TABLE bronze.erp_cust_az12;
COPY bronze.erp_cust_az12
FROM '/home/nick/Documents/programming/customer_orders_ELT/data/source_ERP/CUST_AZ12.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', FREEZE true);

TRUNCATE TABLE bronze.erp_loc_a101;
COPY bronze.erp_loc_a101
FROM '/home/nick/Documents/programming/customer_orders_ELT/data/source_ERP/LOC_A101.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', FREEZE true);

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
COPY bronze.erp_px_cat_g1v2
FROM '/home/nick/Documents/programming/customer_orders_ELT/data/source_ERP/PX_CAT_G1V2.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', FREEZE true);
