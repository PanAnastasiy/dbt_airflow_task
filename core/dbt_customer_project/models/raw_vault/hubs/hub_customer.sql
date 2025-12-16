{{ config(materialized='incremental') }}

SELECT
    CUSTOMER_HK,
    CUSTOMER_ID,
    LOAD_DATE,
    RECORD_SOURCE
FROM {{ ref('stg_tpch_customer') }} s
{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.CUSTOMER_HK = s.CUSTOMER_HK
    )
    {% endif %}
