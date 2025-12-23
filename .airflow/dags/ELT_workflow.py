from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk.bases.operator import chain
from pyhere import here

################################################################################
#   Define parameters and raw data source details
################################################################################

default_args = {
    "owner": "Nick",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

bronze_table_definitions = {
    'bronze.crm_cust_info': {
        'cst_id': 'TEXT',
        'cst_key': 'TEXT',
        'cst_firstname': 'TEXT',
        'cst_lastname': 'TEXT',
        'cst_marital_status': 'TEXT',
        'cst_gndr': 'TEXT',
        'cst_create_date': 'TEXT'
    },
    'bronze.crm_prd_info': {
        'prd_id': 'TEXT',
        'prd_key': 'TEXT',
        'prd_nm': 'TEXT',
        'prd_cost': 'TEXT',
        'prd_line': 'TEXT',
        'prd_start_dt': 'TEXT',
        'prd_end_dt': 'TEXT'
    },
    'bronze.crm_sales_details': {
        'sls_ord_num': 'TEXT',
        'sls_prd_key': 'TEXT',
        'sls_cust_id': 'TEXT',
        'sls_order_dt': 'TEXT',
        'sls_ship_dt': 'TEXT',
        'sls_due_dt': 'TEXT',
        'sls_sales': 'INT',
        'sls_quantity': 'INT',
        'sls_price': 'INT'
    },
    'bronze.erp_cust_az12': {
        'CID': 'TEXT',
        'BDATE': 'TEXT',
        'GEN': 'TEXT'
    },
    'bronze.erp_loc_a101': {
        'CID': 'TEXT',
        'CNTRY': 'TEXT'
    },
    'bronze.erp_px_cat_g1v2': {
        'ID': 'TEXT',
        'CAT': 'TEXT',
        'SUBCAT': 'TEXT',
        'MAINTENANCE': 'TEXT'
    }
}

source_data_paths = {
    'bronze.crm_cust_info': '/var/lib/postgresql/imports/cust_info.csv',
    'bronze.crm_prd_info': '/var/lib/postgresql/imports/prd_info.csv',
    'bronze.crm_sales_details': '/var/lib/postgresql/imports/sales_details.csv',
    'bronze.erp_cust_az12': '/var/lib/postgresql/imports/CUST_AZ12.csv',
    'bronze.erp_loc_a101': '/var/lib/postgresql/imports/LOC_A101.csv',
    'bronze.erp_px_cat_g1v2': '/var/lib/postgresql/imports/PX_CAT_G1V2.csv'
}

################################################################################
#   Define helper functions for generating SQL code
################################################################################

#   Given a table name and column specifications (column name + type), generate
#   a string of SQL code to idempotently create the table
def sql_for_creating_table(table_name: str, col_specs: dict) -> str:
    #   For idempotency, drop (if exists) then recreate the table
    sql_str = f'DROP TABLE IF EXISTS {table_name};\n'
    sql_str += f'CREATE TABLE {table_name} (\n'
    
    #   Now add each column and type
    for col_name, col_type in list(col_specs.items())[:-1]:
        sql_str += f'    {col_name} {col_type},\n'
    col_name, col_type = list(col_specs.items())[-1]
    sql_str += f'    {col_name} {col_type}\n);\n'
    
    return sql_str

#   Given a table name and path to a CSV file (accessible to the Postgres
#   server), return SQL to bulk insert the CSV contents into the table.
#   Assumes the table exists!
def sql_for_bulk_insert(table_name: str, csv_path: str) -> str:
    sql_str = f'TRUNCATE TABLE {table_name};\n'
    sql_str += f"COPY {table_name} FROM '{csv_path}' WITH (FORMAT csv, HEADER true, DELIMITER ',');\n"
    return sql_str

################################################################################
#   Generate SQL code for the bronze layer
################################################################################

#   Generate lines of SQL code to generate all tables
creation_str = ''
for table_name, col_specs in bronze_table_definitions.items():
    creation_str += sql_for_creating_table(table_name, col_specs) + '\n'

#   Generate lines of SQL code to bulk insert all source data
populate_str = ''
for table_name, csv_path in source_data_paths.items():
    populate_str += sql_for_bulk_insert(table_name, csv_path) + '\n'

################################################################################
#   Define the DAG
################################################################################

with DAG(
        dag_id="elt_workflow",
        start_date=datetime(2025, 12, 23),
        schedule="@daily",
        description="The full ELT pipeline for this project",
        default_args=default_args
    ) as dag:
    #---------------------------------------------------------------------------
    #   Bronze layer: ingest data from CSVs
    #---------------------------------------------------------------------------

    bronze_init_tables = SQLExecuteQueryOperator(
        task_id="bronze_init_tables",
        conn_id="postgres_localhost",
        sql=creation_str
    )

    bronze_populate_tables = SQLExecuteQueryOperator(
        task_id="bronze_populate_tables",
        conn_id="postgres_localhost",
        sql=populate_str
    )

    #---------------------------------------------------------------------------
    #   Silver layer: transform data using dbt
    #---------------------------------------------------------------------------

    silver_dbt_run = BashOperator(
        task_id="silver_dbt_run",
        bash_command="source .venv/bin/activate; dbt run --select silver",
        cwd=here()
    )

    silver_dbt_test = BashOperator(
        task_id="silver_dbt_test",
        bash_command="source .venv/bin/activate; dbt test --select silver",
        cwd=here()
    )

    #---------------------------------------------------------------------------
    #   Gold layer: use dbt to join into dimension and fact views
    #---------------------------------------------------------------------------

    gold_dbt_run = BashOperator(
        task_id="gold_dbt_run",
        bash_command="source .venv/bin/activate; dbt run --select gold",
        cwd=here()
    )

    gold_dbt_test = BashOperator(
        task_id="gold_dbt_test",
        bash_command="source .venv/bin/activate; dbt test --select gold",
        cwd=here()
    )

    #   Full ELT pipeline
    chain(
        bronze_init_tables,
        bronze_populate_tables,
        silver_dbt_run,
        silver_dbt_test,
        gold_dbt_run,
        gold_dbt_test
    )
