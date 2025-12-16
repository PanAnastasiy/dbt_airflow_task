{{ config(materialized='incremental') }}

SELECT
    ORDER_HK,
    ORDER_ID,
    LOAD_DATE,
    RECORD_SOURCE
FROM {{ ref('stg_tpch_orders') }} s
{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.ORDER_HK = s.ORDER_HK
    )
    {% endif %}
