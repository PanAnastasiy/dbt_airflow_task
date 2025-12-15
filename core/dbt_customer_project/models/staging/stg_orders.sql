{{ config(materialized='view') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['O_ORDERKEY']) }} as ORDER_HK,
    {{ dbt_utils.generate_surrogate_key(['O_CUSTKEY']) }} as CUSTOMER_HK,
    {{ dbt_utils.generate_surrogate_key(['O_CUSTKEY', 'O_ORDERKEY']) }} as LNK_CUST_ORDER_HK,
    O_ORDERKEY as ORDER_ID,
    O_CUSTKEY as CUSTOMER_ID,
    O_ORDERSTATUS,
    O_TOTALPRICE,
    O_ORDERDATE,
    'TPCH_SF1' as RECORD_SOURCE,
    current_timestamp as LOAD_DATE
FROM {{ source('snowflake_sample', 'orders') }}