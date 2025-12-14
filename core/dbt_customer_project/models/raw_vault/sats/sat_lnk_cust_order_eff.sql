{{ config(materialized='incremental') }}

SELECT
    LNK_CUST_ORDER_HK,
    LOAD_DATE as START_DATE,
    NULL as END_DATE,
    CASE WHEN O_ORDERSTATUS = 'O' THEN TRUE ELSE FALSE END as IS_ACTIVE,
    RECORD_SOURCE,
    LOAD_DATE
FROM {{ ref('stg_tpch_orders') }}