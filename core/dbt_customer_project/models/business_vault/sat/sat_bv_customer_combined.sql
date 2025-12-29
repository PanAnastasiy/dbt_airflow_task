{{ config(materialized='incremental') }}

WITH link AS (
    SELECT
          l.LINK_CUST_ORDER_HK
        , l.CUSTOMER_HK
        , l.ORDER_HK
        , l.LOAD_DTS AS link_load_date
        , l.RECORD_SOURCE AS link_record_source
    FROM {{ ref('link_customer_order') }} AS l
),

stage AS (
    SELECT DISTINCT
          LINK.LINK_CUST_ORDER_HK
        , LINK.CUSTOMER_HK
        , LINK.ORDER_HK
        , o.LOAD_DTS AS START_DATE
        , o.RECORD_SOURCE AS RECORD_SOURCE
    FROM link AS LINK
    INNER JOIN {{ ref('stg_orders') }} AS o
        ON LINK.ORDER_HK = o.ORDER_HK
),

latest_records AS (
    SELECT
          LINK_CUST_ORDER_HK
        , CUSTOMER_HK
        , ORDER_HK
        , START_DATE
        , RECORD_SOURCE
        , ROW_NUMBER() OVER (
              PARTITION BY LINK_CUST_ORDER_HK
              ORDER BY START_DATE DESC
          ) AS rn
    FROM stage
)

SELECT
    LINK_CUST_ORDER_HK
     , CUSTOMER_HK
     , ORDER_HK
     , START_DATE
     , NULL::TIMESTAMP AS END_DATE
    , RECORD_SOURCE
FROM latest_records
WHERE rn = 1
    {% if is_incremental() %}
  AND START_DATE > (
      SELECT MAX(START_DATE)
      FROM {{ this }}
  )
{% endif %}