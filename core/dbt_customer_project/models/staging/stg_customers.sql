{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('snowflake_sample', 'customer') }}
)
SELECT
    -- 1. Hash Keys (HK)
    {{ dbt_utils.generate_surrogate_key(['C_CUSTKEY']) }} as CUSTOMER_HK,

    -- 2. Business Keys (BK)
    C_CUSTKEY as CUSTOMER_ID,

    -- 3. Payload для Slow Satellite (Редко меняются)
    C_NAME,
    C_ADDRESS,
    C_PHONE,
    C_MKTSEGMENT,
    C_NATIONKEY,

    -- 4. Payload для Fast Satellite (Часто меняются - Баланс)
    C_ACCTBAL,

    -- 5. Метаданные
    'TPCH_SF1' as RECORD_SOURCE,
    current_timestamp as LOAD_DATE
FROM source