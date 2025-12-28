{{ config(materialized='view') }}

WITH SOURCE AS (
    SELECT * FROM {{ ref('vip_customers') }}
)

SELECT
    CUSTOMER_ID,
    {{ dbt_utils.generate_surrogate_key(['CUSTOMER_ID']) }} AS CUSTOMER_HK,
    {{ dbt_utils.generate_surrogate_key(['VIP_STATUS']) }} AS VIP_HASHDIFF,
    JOINED_VIP_DATE::TIMESTAMP AS LOAD_DTS,
    JOINED_VIP_DATE::TIMESTAMP AS EFFECTIVE_FROM,
    'MANUAL_CSV' AS RECORD_SOURCE,
    VIP_STATUS
FROM SOURCE
