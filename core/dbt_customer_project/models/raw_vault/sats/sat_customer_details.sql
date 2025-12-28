{{ config(
    materialized='incremental'
) }}

WITH source_data AS (
    SELECT
        CUSTOMER_HK,
        CUSTOMER_HASHDIFF,
        first_name,  -- <--- ВАЖНО: Тут должно быть first_name, а не c_name
        phone,
        address,
        segment,
        LOAD_DTS,
        EFFECTIVE_FROM,
        RECORD_SOURCE
    FROM {{ ref('stg_customers') }}
)

SELECT * FROM source_data src
    {% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1 FROM {{ this }} tgt
    WHERE tgt.CUSTOMER_HK = src.CUSTOMER_HK
  AND tgt.CUSTOMER_HASHDIFF = src.CUSTOMER_HASHDIFF
    )
    {% endif %}