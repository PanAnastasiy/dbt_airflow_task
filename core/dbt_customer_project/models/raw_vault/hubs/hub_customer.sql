{{ config(materialized='incremental') }}

SELECT DISTINCT
    CUSTOMER_HK,
    CUSTOMER_ID,
    LOAD_DATE,
    RECORD_SOURCE
FROM {{ ref('stg_tpch_customer') }}
    {% if is_incremental() %}
WHERE LOAD_DATE > (SELECT MAX(LOAD_DATE) FROM {{ this }})
    {% endif %}
