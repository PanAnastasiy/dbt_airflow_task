{{ config(materialized='table') }}

SELECT
    link.LINK_CUST_ORDER_HK,
    link.ORDER_HK,
    link.CUSTOMER_HK,

    -- Исправлено имя колонки (date_day вместо date_id)
    d.date_day as date_key,

    -- Пример мер (нужно взять из стейджа или сателлита)
    stg.total_amount,

    link.LOAD_DTS as ingestion_date

FROM {{ ref('link_customer_order') }} link

-- Джойним дату (конвертируем timestamp в date для джойна)
LEFT JOIN {{ ref('dim_date') }} d
ON CAST(link.LOAD_DTS AS DATE) = d.date_day

-- Джойним детали заказа для суммы (из стейджа или сателлита)
    LEFT JOIN {{ ref('stg_orders') }} stg
    ON link.ORDER_HK = stg.ORDER_HK