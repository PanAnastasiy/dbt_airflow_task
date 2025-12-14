{{ config(materialized='incremental', unique_key='CUSTOMER_PK') }}

WITH source AS (
    SELECT
        CUSTOMER_PK,
        LOAD_DATE,
        RECORD_SOURCE,
        C_NAME,
        C_ADDRESS,
        C_PHONE,
        C_ACCTBAL,
        C_MKTSEGMENT,


        MD5(
            CAST(C_NAME AS VARCHAR) ||
            CAST(C_ADDRESS AS VARCHAR) ||
            CAST(C_PHONE AS VARCHAR) ||
            CAST(C_ACCTBAL AS VARCHAR) ||
            CAST(C_MKTSEGMENT AS VARCHAR)
        ) AS HASHDIFF

    FROM {{ ref('stg_customers') }}
)

SELECT * FROM source
    {% if is_incremental() %}
WHERE HASHDIFF NOT IN (SELECT HASHDIFF FROM {{ this }})
    {% endif %}
