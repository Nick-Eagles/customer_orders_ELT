# Customer Orders ELT Project

## Software Set-up

Much of this project uses Postgres SQL code executed using `Airflow` and `dbt`,
both of which were installed in a virtual environment with
`uv`. During development, I often used `DBeaver` to interact with my
local Postgres database.

```
#   Use uv to set up a virtual environment with dbt for Postgres and Airflow
uv venv --python 3.10
uv pip install dbt-core==1.11.2 dbt-postgres
uv pip install "apache-airflow[celery]==3.1.5" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.5/constraints-3.10.txt"
uv pip install apache-airflow-providers-postgres
uv pip install pyhere

#   Activate the venv and set an environment variable
source .venv/bin/activate
export AIRFLOW_HOME=$(pwd)/.airflow

#   Since the repo already exists, I do a workaround to get `dbt` files at the
#   top level of the repo
dbt init customer_orders_elt
cp -R customer_orders_elt/* .
rm -r customer_orders_elt

#   Since I'm doing everything locally, I set up the data sources so they're
#   accessible to the Postgres server
sudo cp data/source_CRM/* /var/lib/postgresql/imports/
sudo cp data/source_ERP/* /var/lib/postgresql/imports/
```
