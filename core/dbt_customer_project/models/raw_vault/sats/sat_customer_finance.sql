{{ config(materialized='incremental') }}

WITH source AS (
    SELECT
        CUSTOMER_HK,
        C_ACCTBAL,
        LOAD_DATE,
        RECORD_SOURCE,
        {{ dbt_utils.generate_surrogate_key(['C_ACCTBAL']) }} AS HASH_DIFF
    FROM {{ ref('stg_tpch_customer') }}
)
SELECT
    CUSTOMER_HK,
    C_ACCTBAL,
    LOAD_DATE,
    RECORD_SOURCE,
    HASH_DIFF
FROM source s
    {% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.CUSTOMER_HK = s.CUSTOMER_HK
  AND t.HASH_DIFF = s.HASH_DIFF
    )
    {% endif %}
