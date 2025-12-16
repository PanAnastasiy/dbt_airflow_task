{{ config(materialized='incremental') }}

SELECT
    LNK_CUST_ORDER_HK,
    LOAD_DATE AS START_DATE,
    NULL AS END_DATE,
    RECORD_SOURCE
FROM {{ ref('stg_tpch_orders') }} s
{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.LNK_CUST_ORDER_HK = s.LNK_CUST_ORDER_HK
  AND t.START_DATE = s.LOAD_DATE
    )
    {% endif %}
