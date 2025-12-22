{{ config(materialized='view') }}

SELECT
    sales.sls_ord_num AS order_number,
    customer.surrogate_key AS customer_key,
    product.surrogate_key AS product_key,
    sales.sls_order_dt AS order_date,
    sales.sls_ship_dt AS ship_date,
    sales.sls_due_dt AS due_date,
    sales.sls_sales AS revenue,
    sales.sls_quantity AS quantity_sold,
    sales.sls_price AS price
FROM {{ ref('crm_sales_details') }} AS sales
LEFT JOIN {{ ref('dim_customer') }} AS customer
    ON sales.sls_cust_id = customer.crm_key_1
LEFT JOIN {{ ref('dim_product') }} AS product
    ON sales.sls_prd_key = product.crm_key_3
