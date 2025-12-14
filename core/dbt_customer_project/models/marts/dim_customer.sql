{{ config(materialized='table') }}
SELECT
    CUSTOMER_HK as DIM_CUSTOMER_KEY,
    C_NAME,
    CLIENT_SEGMENT,
    MANAGER_NAME,
    C_ACCTBAL
FROM {{ ref('sat_bv_customer_combined') }}