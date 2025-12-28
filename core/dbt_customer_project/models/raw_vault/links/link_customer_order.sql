{{ config(materialized='incremental') }}

SELECT DISTINCT
    LINK_CUST_ORDER_HK,
    CUSTOMER_HK,
    ORDER_HK,
    LOAD_DTS,
    RECORD_SOURCE
FROM {{ ref('stg_orders') }}
WHERE LINK_CUST_ORDER_HK IS NOT NULL
    {% if is_incremental() %}
  AND LOAD_DTS > (SELECT MAX(LOAD_DTS) FROM {{ this }})
{% endif %}
