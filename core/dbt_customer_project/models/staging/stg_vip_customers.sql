{{ config(materialized='view') }}
SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as CUSTOMER_HK,
    customer_id as CUSTOMER_ID,
    vip_category,
    assigned_manager,
    'MANUAL_SEED' as RECORD_SOURCE,
    current_timestamp as LOAD_DATE
FROM {{ ref('vip_customers') }}