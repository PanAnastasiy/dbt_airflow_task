{{ config(materialized='incremental') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['CUSTOMER_HK', 'ORDER_HK']) }} AS LINK_HK,
    CUSTOMER_HK,
    ORDER_HK,
    LOAD_DATE,
    RECORD_SOURCE
FROM {{ ref('stg_tpch_orders') }} s
    {% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.LINK_HK = {{ dbt_utils.generate_surrogate_key(['s.CUSTOMER_HK', 's.ORDER_HK']) }}
    )
    {% endif %}
