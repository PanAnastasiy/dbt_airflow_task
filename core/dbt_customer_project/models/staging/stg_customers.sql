{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('snowflake_sample', 'customer') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['C_CUSTKEY']) }} as CUSTOMER_HK,
    C_CUSTKEY as CUSTOMER_ID,

    C_NAME,
    C_ADDRESS,
    C_PHONE,
    C_MKTSEGMENT,
    C_NATIONKEY,
    C_ACCTBAL,
    'TPCH_SF1' as RECORD_SOURCE,
    current_timestamp as LOAD_DATE
FROM source