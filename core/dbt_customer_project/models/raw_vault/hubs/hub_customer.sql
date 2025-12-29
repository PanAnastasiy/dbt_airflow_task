{{ config(materialized='incremental') }}

SELECT DISTINCT
    CUSTOMER_HK
              , customer_id
              , LOAD_DTS
              , RECORD_SOURCE
FROM {{ ref('stg_customers') }}
WHERE CUSTOMER_HK IS NOT NULL
    {% if is_incremental() %}
  AND LOAD_DTS > (
      SELECT MAX(LOAD_DTS)
      FROM {{ this }}
  )
{% endif %}
