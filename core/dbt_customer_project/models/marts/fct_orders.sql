{{ config(materialized='table') }}

SELECT
    o.ORDER_HK as FACT_KEY,
    o.O_ORDERDATE as DATE_KEY,
    o.CUSTOMER_HK as CUSTOMER_KEY,
    o.O_TOTALPRICE,
    o.O_ORDERSTATUS,
    eff.IS_ACTIVE
FROM {{ ref('stg_tpch_orders') }} o
LEFT JOIN {{ ref('sat_lnk_cust_order_eff') }} eff
ON o.LNK_CUST_ORDER_HK = eff.LNK_CUST_ORDER_HK