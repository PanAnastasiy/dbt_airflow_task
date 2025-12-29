{{ config(materialized='incremental') }}

WITH link AS (
    SELECT
        LINK_CUST_ORDER_HK,
        CUSTOMER_HK,
        ORDER_HK
    FROM {{ ref('link_customer_order') }}
),

stage AS (
    SELECT DISTINCT
        LINK.LINK_CUST_ORDER_HK,
        LINK.LINK_CUST_ORDER_HK AS DRIVING_KEY,
        LINK.CUSTOMER_HK,
        LINK.ORDER_HK,
        O.LOAD_DTS AS START_DATE,
        O.RECORD_SOURCE
    FROM link AS LINK
    INNER JOIN {{ ref('stg_orders') }} AS O
        ON LINK.ORDER_HK = O.ORDER_HK
),

latest_records AS (
    SELECT
        LINK_CUST_ORDER_HK,
        DRIVING_KEY,
        CUSTOMER_HK,
        ORDER_HK,
        START_DATE,
        RECORD_SOURCE,
        ROW_NUMBER() OVER (
            PARTITION BY DRIVING_KEY
            ORDER BY START_DATE DESC
        ) AS rn
    FROM stage
)

SELECT
    LINK_CUST_ORDER_HK,
    DRIVING_KEY,
    CUSTOMER_HK,
    ORDER_HK,
    START_DATE,
    NULL::TIMESTAMP AS END_DATE,
    RECORD_SOURCE
FROM latest_records
WHERE rn = 1

    {% if is_incremental() %}
    AND START_DATE > (
        SELECT MAX(START_DATE)
        FROM {{ this }}
    )
{% endif %}