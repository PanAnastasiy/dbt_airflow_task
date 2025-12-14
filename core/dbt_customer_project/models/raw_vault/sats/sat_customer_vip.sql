{{ config(materialized='incremental') }}

WITH source AS (
    SELECT
        CUSTOMER_HK, vip_category, assigned_manager, LOAD_DATE, RECORD_SOURCE,
        {{ dbt_utils.generate_surrogate_key(['vip_category', 'assigned_manager']) }} as HASH_DIFF
    FROM {{ ref('stg_seed_vip') }}
)
SELECT * FROM source
    {% if is_incremental() %}
WHERE HASH_DIFF NOT IN (SELECT HASH_DIFF FROM {{ this }} WHERE CUSTOMER_HK = source.CUSTOMER_HK)
    {% endif %}