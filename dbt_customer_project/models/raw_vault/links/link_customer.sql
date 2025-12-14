{{ config(materialized='incremental', unique_key='LINK_PK') }}

WITH source AS (
    SELECT
        MD5(CAST(O_CUSTKEY AS VARCHAR) || CAST(O_ORDERKEY AS VARCHAR)) AS LINK_PK,
        MD5(CAST(O_CUSTKEY AS VARCHAR)) AS CUSTOMER_PK,
        MD5(CAST(O_ORDERKEY AS VARCHAR)) AS ORDER_PK,
        O_ORDERDATE AS LOAD_DATE,
        'SNOWFLAKE_SAMPLE' AS RECORD_SOURCE
    FROM {{ source('tpch', 'orders') }}

)

SELECT * FROM source
    {% if is_incremental() %}

WHERE LINK_PK NOT IN (SELECT LINK_PK FROM {{ this }})
    {% endif %}