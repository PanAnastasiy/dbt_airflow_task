{{ config(materialized='view') }}

WITH source AS (
    SELECT
        c_custkey as customer_id,
        c_name,
        c_phone,
        c_address,
        c_acctbal,   -- <--- ДОБАВИЛИ БАЛАНС
        c_mktsegment, -- <--- ДОБАВИЛИ СЕГМЕНТ (нужен для BV логики)
        c_nationkey,
        '2024-01-01'::TIMESTAMP as load_date
    FROM {{ source('tpch', 'customer') }}
)

SELECT
    customer_id,
    c_nationkey,

    -- Hash Keys
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS CUSTOMER_HK,

    -- Hash Diff (Details)
    {{ dbt_utils.generate_surrogate_key(['c_name', 'c_phone', 'c_address']) }} AS CUSTOMER_HASHDIFF,

    -- Hash Diff (Finance) - Добавили хеш для финансов
    {{ dbt_utils.generate_surrogate_key(['c_acctbal']) }} AS HASHDIFF_FINANCE,

    -- Meta
    load_date AS LOAD_DTS,
    load_date AS EFFECTIVE_FROM,
    'TPCH' AS RECORD_SOURCE,

    -- Payload (Renamed to clean names)
    c_name as first_name,      -- <--- ПЕРЕИМЕНОВАЛИ
    c_phone as phone,          -- <--- ПЕРЕИМЕНОВАЛИ
    c_address as address,      -- <--- ПЕРЕИМЕНОВАЛИ
    c_acctbal as account_balance, -- <--- НОВАЯ КОЛОНКА
    c_mktsegment as segment       -- <--- НОВАЯ КОЛОНКА

FROM source