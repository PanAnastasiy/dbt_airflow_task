{{ config(materialized='table') }}

SELECT pit.CUSTOMER_HK,
       pit.AS_OF_DATE AS valid_from,

       COALESCE(
               LEAD(pit.AS_OF_DATE) OVER(PARTITION BY pit.CUSTOMER_HK ORDER BY pit.AS_OF_DATE),
               CAST('9999-12-31' AS TIMESTAMP)
       )              AS valid_to,
       bv.CUSTOMER_HK AS customer_id,

       bv.RECORD_SOURCE

FROM {{ ref('pit_customer') }} AS pit

LEFT JOIN {{ ref('sat_bv_customer_combined') }} AS bv
ON pit.CUSTOMER_HK = bv.CUSTOMER_HK
    AND pit.SAT_BV_COMBINED_LOAD_DTS = bv.START_DATE
LEFT JOIN {{ ref ('sat_eff_order_customer') }} AS eff
ON pit.CUSTOMER_HK = eff.CUSTOMER_HK
    AND pit.SAT_EFF_LOAD_DTS = eff.START_DATE