{{ config(materialized='incremental') }}

WITH source AS (
    SELECT
        CUSTOMER_HK, C_ACCTBAL, LOAD_DATE, RECORD_SOURCE,
        {{ dbt_utils.generate_surrogate_key(['C_ACCTBAL']) }} as HASH_DIFF
    FROM {{ ref('stg_tpch_customer') }}
)
SELECT * FROM source
    {% if is_incremental() %}
WHERE HASH_DIFF NOT IN (SELECT HASH_DIFF FROM {{ this }} WHERE CUSTOMER_HK = source.CUSTOMER_HK)
    {% endif %}