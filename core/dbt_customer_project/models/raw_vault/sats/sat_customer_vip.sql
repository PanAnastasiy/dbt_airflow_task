{{ config(
    materialized='incremental'
) }}

WITH source_data AS (
    SELECT
        CUSTOMER_HK,
        VIP_HASHDIFF,
        vip_status,
        LOAD_DTS,
        EFFECTIVE_FROM,
        RECORD_SOURCE
    FROM {{ ref('stg_vip_customers') }}
)

SELECT * FROM source_data src
    {% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1 FROM {{ this }} tgt
    WHERE tgt.CUSTOMER_HK = src.CUSTOMER_HK
  AND tgt.VIP_HASHDIFF = src.VIP_HASHDIFF
    )
    {% endif %}
