{{ config(materialized='view') }}

WITH source AS (
    SELECT
        o_orderkey as order_id,
        o_custkey as customer_id,
        o_orderstatus as order_status,
        o_totalprice as total_amount,
        o_orderdate as order_date,
        'TPCH' as record_source
    FROM {{ source('tpch', 'orders') }}
)

SELECT
    order_id,
    customer_id,

    -- Hash Keys
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS ORDER_HK,
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS CUSTOMER_HK,

    -- Link Hash Key (Зависит от ключей Хабов)
    {{ dbt_utils.generate_surrogate_key(['order_id', 'customer_id']) }} AS LINK_CUST_ORDER_HK,

    -- Hash Diff (Атрибуты заказа)
    {{ dbt_utils.generate_surrogate_key([
    'order_status',
    'total_amount'
    ]) }} AS ORDER_HASHDIFF,

    -- Meta
    order_date AS LOAD_DTS,
    order_date AS EFFECTIVE_FROM,
    record_source AS RECORD_SOURCE,

    order_status,
    total_amount

FROM source