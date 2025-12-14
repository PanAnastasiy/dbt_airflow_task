{{ config(materialized='table') }}

SELECT
    h.CUSTOMER_HK,
    MAX(s1.LOAD_DATE) as SAT_DETAILS_LD,
    MAX(s2.LOAD_DATE) as SAT_FINANCE_LD,
    MAX(s3.LOAD_DATE) as SAT_VIP_LD,
    current_timestamp as PIT_LOAD_DATE
FROM {{ ref('hub_customer') }} h
LEFT JOIN {{ ref('sat_customer_details') }} s1 ON h.CUSTOMER_HK = s1.CUSTOMER_HK
    LEFT JOIN {{ ref('sat_customer_finance') }} s2 ON h.CUSTOMER_HK = s2.CUSTOMER_HK
    LEFT JOIN {{ ref('sat_customer_vip') }} s3     ON h.CUSTOMER_HK = s3.CUSTOMER_HK
GROUP BY h.CUSTOMER_HK