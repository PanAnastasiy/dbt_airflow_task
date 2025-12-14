{{ config(materialized='view') }}

SELECT
    pit.CUSTOMER_HK,
    s1.C_NAME,
    s1.C_ADDRESS,
    s2.C_ACCTBAL,
    -- ЛОГИКА БИЗНЕС ВОЛТА: Если есть VIP запись, берем её, иначе 'Standard'
    COALESCE(s3.vip_category, 'Standard') as CLIENT_SEGMENT,
    COALESCE(s3.assigned_manager, 'General Support') as MANAGER_NAME
FROM {{ ref('pit_customer') }} pit
-- Джойним по ключу И по дате из PIT таблицы (ghost records optimization)
LEFT JOIN {{ ref('sat_customer_details') }} s1
ON pit.CUSTOMER_HK = s1.CUSTOMER_HK AND pit.SAT_DETAILS_LD = s1.LOAD_DATE
    LEFT JOIN {{ ref('sat_customer_finance') }} s2
    ON pit.CUSTOMER_HK = s2.CUSTOMER_HK AND pit.SAT_FINANCE_LD = s2.LOAD_DATE
    LEFT JOIN {{ ref('sat_customer_vip') }} s3
    ON pit.CUSTOMER_HK = s3.CUSTOMER_HK AND pit.SAT_VIP_LD = s3.LOAD_DATE