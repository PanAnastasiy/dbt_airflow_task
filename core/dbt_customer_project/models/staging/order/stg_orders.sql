{{ config(materialized='view') }}

WITH SOURCE AS (
    SELECT
        O_ORDERKEY AS ORDER_ID,
        O_CUSTKEY AS CUSTOMER_ID,
        O_ORDERSTATUS AS ORDER_STATUS,
        O_TOTALPRICE AS TOTAL_AMOUNT,
        O_ORDERDATE AS ORDER_DATE,
        'TPCH' AS RECORD_SOURCE
    FROM {{ source('tpch', 'orders') }}
)

SELECT
    ORDER_ID,
    CUSTOMER_ID,
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS ORDER_HK,
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS CUSTOMER_HK,
    {{ dbt_utils.generate_surrogate_key(['order_id', 'customer_id']) }} AS LINK_CUST_ORDER_HK,
    {{ dbt_utils.generate_surrogate_key(['order_status', 'total_amount']) }} AS ORDER_HASHDIFF,
    ORDER_DATE AS LOAD_DTS,
    ORDER_DATE AS EFFECTIVE_FROM,
    RECORD_SOURCE,
    ORDER_STATUS,
    TOTAL_AMOUNT
FROM SOURCE
