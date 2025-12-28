{{ config(materialized='incremental') }}

WITH source AS (
    SELECT
        CUSTOMER_HK,
        HASHDIFF_FINANCE, -- Этот хеш мы теперь генерим в стейдже
        account_balance,  -- <--- ТЕПЕРЬ ОНА ЕСТЬ
        '10000' as credit_limit, -- Заглушка (нет в TPCH)
        LOAD_DTS,
        EFFECTIVE_FROM,
        RECORD_SOURCE
    FROM {{ ref('stg_customers') }}
)

SELECT * FROM source src
    {% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} tgt
    WHERE tgt.CUSTOMER_HK = src.CUSTOMER_HK
  AND tgt.HASHDIFF_FINANCE = src.HASHDIFF_FINANCE
    )
    {% endif %}