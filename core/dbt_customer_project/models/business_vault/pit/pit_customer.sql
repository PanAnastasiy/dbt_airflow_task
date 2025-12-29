{{ config(materialized='table') }}

WITH
bv_combined AS (
    SELECT
        CUSTOMER_HK,
        START_DATE,
        START_DATE AS LOAD_DTS,
        COALESCE(
            LEAD(START_DATE) OVER (PARTITION BY CUSTOMER_HK ORDER BY START_DATE),
            TO_TIMESTAMP('9999-12-31')
        ) AS END_DATE
    FROM {{ ref('sat_bv_customer_combined') }}
),

bv_eff AS (
    SELECT
        CUSTOMER_HK,
        START_DATE,
        START_DATE AS LOAD_DTS,
        COALESCE(
            LEAD(START_DATE) OVER (PARTITION BY CUSTOMER_HK ORDER BY START_DATE),
            TO_TIMESTAMP('9999-12-31')
        ) AS END_DATE
    FROM {{ ref('sat_eff_order_customer') }}
),

all_dates AS (
    SELECT CUSTOMER_HK, START_DATE AS AS_OF_DATE FROM bv_combined
    UNION DISTINCT
    SELECT CUSTOMER_HK, START_DATE FROM bv_eff
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['d.CUSTOMER_HK', 'd.AS_OF_DATE']) }} AS PIT_HK,
    d.CUSTOMER_HK,
    d.AS_OF_DATE,

    c.LOAD_DTS AS SAT_BV_COMBINED_LOAD_DTS,
    e.LOAD_DTS AS SAT_EFF_LOAD_DTS

FROM all_dates d

    INNER JOIN bv_combined c
ON d.CUSTOMER_HK = c.CUSTOMER_HK
    AND d.AS_OF_DATE >= c.START_DATE
    AND d.AS_OF_DATE < c.END_DATE
    LEFT JOIN bv_eff e
    ON d.CUSTOMER_HK = e.CUSTOMER_HK
    AND d.AS_OF_DATE >= e.START_DATE
    AND d.AS_OF_DATE < e.END_DATE