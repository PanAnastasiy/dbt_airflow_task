{{ config(materialized='view') }}



SELECT

    MD5(CAST(C_CUSTKEY AS VARCHAR)) AS CUSTOMER_PK,


    C_CUSTKEY AS CUSTOMER_ID,

    CURRENT_DATE() AS LOAD_DATE,
    'SNOWFLAKE_SAMPLE' AS RECORD_SOURCE,

    C_NAME,
    C_ADDRESS,
    C_PHONE,
    C_ACCTBAL,
    C_MKTSEGMENT

FROM {{ source('tpch', 'customer') }}


