{{ config(materialized='incremental') }}

/*
   Effective Satellite на Линку.
   Driving Key (Ведущий ключ): ORDER_HK (У одного заказа один клиент).
*/

WITH stage AS (
    SELECT
        LINK_CUST_ORDER_HK,
        ORDER_HK AS DRIVING_KEY, -- Заказ - главный объект связи
        LOAD_DTS,
        EFFECTIVE_FROM,
        RECORD_SOURCE
    FROM {{ ref('stg_orders') }}
),

latest_records AS (
    SELECT
        LINK_CUST_ORDER_HK,
        DRIVING_KEY,
        EFFECTIVE_FROM,
        LOAD_DTS,
        RECORD_SOURCE,
        -- Берем последнюю запись для каждого заказа
        ROW_NUMBER() OVER (
            PARTITION BY DRIVING_KEY
            ORDER BY EFFECTIVE_FROM DESC, LOAD_DTS DESC
        ) as rn
    FROM stage
)

SELECT
    LINK_CUST_ORDER_HK,
    DRIVING_KEY,
    EFFECTIVE_FROM AS START_DATE,
    '9999-12-31'::TIMESTAMP AS END_DATE, -- В Raw/BV храним открытую дату
    LOAD_DTS,
    RECORD_SOURCE
FROM latest_records
WHERE rn = 1

    {% if is_incremental() %}
  AND LOAD_DTS > (SELECT MAX(LOAD_DTS) FROM {{ this }})
{% endif %}
