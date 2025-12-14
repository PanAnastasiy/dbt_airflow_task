{{ config(materialized='incremental') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['CUSTOMER_HK', 'ORDER_HK']) }} as LINK_HK,
    CUSTOMER_HK,
    ORDER_HK,
    LOAD_DATE,
    RECORD_SOURCE
FROM {{ ref('stg_tpch_orders') }}
    {% if is_incremental() %}
WHERE LOAD_DATE > (SELECT MAX(LOAD_DATE) FROM {{ this }})
    {% endif %}