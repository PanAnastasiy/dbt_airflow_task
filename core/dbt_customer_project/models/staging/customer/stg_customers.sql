{{ config(materialized='view') }}

WITH SOURCE AS (
    SELECT
        C_CUSTKEY AS CUSTOMER_ID,
        C_NAME,
        C_PHONE,
        C_ADDRESS,
        C_ACCTBAL,
        C_MKTSEGMENT,
        C_NATIONKEY,
        '2024-01-01'::TIMESTAMP AS LOAD_DATE
    FROM {{ source('tpch', 'customer') }}
)

SELECT
    CUSTOMER_ID,
    C_NATIONKEY,
    {{ dbt_utils.generate_surrogate_key(['CUSTOMER_ID']) }} AS CUSTOMER_HK,
    {{ dbt_utils.generate_surrogate_key(['C_NAME', 'C_PHONE', 'C_ADDRESS']) }} AS CUSTOMER_HASHDIFF,
    {{ dbt_utils.generate_surrogate_key(['C_ACCTBAL']) }} AS HASHDIFF_FINANCE,
    LOAD_DATE AS LOAD_DTS,
    LOAD_DATE AS EFFECTIVE_FROM,
    'TPCH' AS RECORD_SOURCE,
    C_NAME AS FIRST_NAME,
    C_PHONE AS PHONE,
    C_ADDRESS AS ADDRESS,
    C_ACCTBAL AS ACCOUNT_BALANCE,
    C_MKTSEGMENT AS SEGMENT
FROM SOURCE
