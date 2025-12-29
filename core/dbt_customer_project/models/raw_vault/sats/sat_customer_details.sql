{{ config(materialized='incremental') }}

SELECT DISTINCT
    c.CUSTOMER_HK,
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.PHONE,
    c.ADDRESS,
    c.SEGMENT,
    c.C_NATIONKEY AS NATIONKEY,
    c.LOAD_DTS,
    c.RECORD_SOURCE,
    c.CUSTOMER_HASHDIFF
FROM {{ ref('stg_customers') }} AS c
{% if is_incremental() %}
WHERE c.LOAD_DTS > (SELECT MAX(LOAD_DTS) FROM {{ this }})
    {% endif %}