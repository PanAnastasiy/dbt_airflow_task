{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ ref('vip_customers') }}
)

SELECT
    -- Keys
    customer_id,

    -- Hash Key (PK)
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS CUSTOMER_HK,

    -- Hash Diff (Payload)
    {{ dbt_utils.generate_surrogate_key(['vip_status']) }} AS VIP_HASHDIFF,

    -- Meta
    joined_vip_date::TIMESTAMP AS LOAD_DTS,
    joined_vip_date::TIMESTAMP AS EFFECTIVE_FROM,
    'MANUAL_CSV' AS RECORD_SOURCE,

    -- Payload
    vip_status

FROM source