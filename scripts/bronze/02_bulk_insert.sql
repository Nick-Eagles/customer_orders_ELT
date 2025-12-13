/*
Bulk insert data into Bronze-layer tables from CSV files

After 01_init_tables.sql, this script truncates any existing data, then loads
fresh data from the source CSVs. This script can be run within this repository
with psql, e.g.,

    psql -d customer_orders_elt -U nick -f scripts/bronze/02_bulk_insert.sql
*/

-- #############################################################################
-- #  Load CRM source data
-- #############################################################################

TRUNCATE TABLE bronze.crm_cust_info;
\copy bronze.crm_cust_info FROM 'data/source_CRM/cust_info.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

TRUNCATE TABLE bronze.crm_prd_info;
\copy bronze.crm_prd_info FROM 'data/source_CRM/prd_info.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

TRUNCATE TABLE bronze.crm_sales_details;
\copy bronze.crm_sales_details FROM 'data/source_CRM/sales_details.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

-- #############################################################################
-- #  Load ERP source data
-- #############################################################################

TRUNCATE TABLE bronze.erp_cust_az12;
\copy bronze.erp_cust_az12 FROM 'data/source_ERP/CUST_AZ12.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

TRUNCATE TABLE bronze.erp_loc_a101;
\copy bronze.erp_loc_a101 FROM 'data/source_ERP/LOC_A101.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
\copy bronze.erp_px_cat_g1v2 FROM 'data/source_ERP/PX_CAT_G1V2.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');
