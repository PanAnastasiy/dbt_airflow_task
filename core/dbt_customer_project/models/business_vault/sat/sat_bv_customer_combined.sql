{{ config(materialized='view') }}

WITH pit AS (
    SELECT * FROM {{ ref('pit_customer') }}
    WHERE AS_OF_DATE = CURRENT_DATE
),

raw_sat_main AS (
    SELECT * FROM {{ ref('sat_customer_details') }}
),

raw_sat_vip AS (
    SELECT * FROM {{ ref('sat_customer_vip') }}
)

SELECT
    pit.CUSTOMER_HK,
    pit.AS_OF_DATE,

    -- Данные из Details
    s1.first_name,
    s1.phone,

    -- Данные из VIP (Исправили имя колонки)
    s2.vip_status, -- БЫЛО vip_level, СТАЛО vip_status

    -- Бизнес-логика
    CASE
        WHEN s2.vip_status = 'Platinum' OR s1.segment = 'BUILDING' THEN TRUE
        ELSE FALSE
        END as is_priority_customer,

    pit.AS_OF_DATE as COMBINED_LOAD_DTS

FROM pit
         LEFT JOIN raw_sat_main s1
                   ON pit.CUSTOMER_HK = s1.CUSTOMER_HK
                       AND pit.SAT_DETAILS_LOAD_DTS = s1.LOAD_DTS

         LEFT JOIN raw_sat_vip s2
                   ON pit.CUSTOMER_HK = s2.CUSTOMER_HK
                       AND pit.SAT_VIP_LOAD_DTS = s2.LOAD_DTS