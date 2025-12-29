{{ config(materialized='incremental') }}

WITH SOURCE_DATA AS (
    SELECT
        CUSTOMER_HK,
        VIP_HASHDIFF,
        VIP_STATUS,
        LOAD_DTS,
        EFFECTIVE_FROM,
        RECORD_SOURCE
    FROM {{ ref('stg_vip_customers') }}
)

SELECT
    SRC.CUSTOMER_HK,
    SRC.VIP_HASHDIFF,
    SRC.VIP_STATUS,
    SRC.LOAD_DTS,
    SRC.EFFECTIVE_FROM,
    SRC.RECORD_SOURCE
FROM SOURCE_DATA AS SRC
    {% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS TGT
    WHERE TGT.CUSTOMER_HK = SRC.CUSTOMER_HK
  AND TGT.VIP_HASHDIFF = SRC.VIP_HASHDIFF
    )
    {% endif %}

