{{ config(materialized='table') }}

SELECT
    link.LINK_CUST_ORDER_HK,
    link.ORDER_HK,
    link.CUSTOMER_HK,
    d.date_day AS date_key,
    {{ dbt_utils.generate_surrogate_key(['cust.NATIONKEY']) }} AS state_pk,
    stg.TOTAL_AMOUNT,

    link.LOAD_DTS AS ingestion_date

FROM {{ ref('link_customer_order') }} AS link

    LEFT JOIN {{ ref('dim_date') }} AS d
ON CAST(link.LOAD_DTS AS DATE) = d.date_day

    LEFT JOIN {{ ref('stg_orders') }} AS stg
    ON link.ORDER_HK = stg.ORDER_HK

    LEFT JOIN {{ ref('sat_customer_details') }} AS cust
    ON link.CUSTOMER_HK = cust.CUSTOMER_HK
    QUALIFY ROW_NUMBER() OVER (
    PARTITION BY link.ORDER_HK
    ORDER BY cust.LOAD_DTS DESC
    ) = 1