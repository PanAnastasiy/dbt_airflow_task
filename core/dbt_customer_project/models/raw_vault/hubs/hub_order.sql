{{ config(materialized='incremental') }}

SELECT DISTINCT
    ORDER_HK
              , order_id
              , LOAD_DTS
              , RECORD_SOURCE
FROM {{ ref('stg_orders') }}
WHERE ORDER_HK IS NOT NULL
    {% if is_incremental() %}
  AND LOAD_DTS > (
      SELECT MAX(LOAD_DTS)
      FROM {{ this }}
  )
{% endif %}
