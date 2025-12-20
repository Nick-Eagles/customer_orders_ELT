# Customer Orders ELT Project

## Software Set-up

Much of this project uses Postgres SQL code executed using `dbt` installed
through `uv`. During development, I often used `DBeaver` to interact with my
local Postgres database.

```
uv venv --python 3.10
uv pip install dbt-core dbt-postgres
source .venv/bin/activate

#   Since the repo already exists, I do a workaround to get `dbt` files at the
#   top level of the repo
dbt init customer_orders_elt
cp -R customer_orders_elt/* .
rm -r customer_orders_elt
```
