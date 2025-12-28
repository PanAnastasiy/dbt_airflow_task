{{ config(
    materialized='table'
) }}

WITH snapshot_dates AS (
    -- Собираем все даты изменений из всех 3 сателлитов
    SELECT DISTINCT EFFECTIVE_FROM AS AS_OF_DATE FROM {{ ref('sat_customer_details') }}
    UNION
    SELECT DISTINCT EFFECTIVE_FROM AS AS_OF_DATE FROM {{ ref('sat_customer_finance') }}
    UNION
    SELECT DISTINCT EFFECTIVE_FROM AS AS_OF_DATE FROM {{ ref('sat_customer_vip') }}
),

all_keys AS (
    SELECT DISTINCT CUSTOMER_HK FROM {{ ref('hub_customer') }}
),

pit_logic AS (
    SELECT
        k.CUSTOMER_HK,
        d.AS_OF_DATE,

        -- Логика для Details
        sd.LOAD_DTS AS LOAD_DTS_DETAILS,
        DENSE_RANK() OVER (
            PARTITION BY k.CUSTOMER_HK, d.AS_OF_DATE
            ORDER BY sd.EFFECTIVE_FROM DESC
        ) as rn_details,

        -- Логика для Finance (ДОБАВИЛИ ЭТОТ БЛОК)
        sf.LOAD_DTS AS LOAD_DTS_FINANCE,
        DENSE_RANK() OVER (
            PARTITION BY k.CUSTOMER_HK, d.AS_OF_DATE
            ORDER BY sf.EFFECTIVE_FROM DESC
        ) as rn_finance,

        -- Логика для VIP
        sv.LOAD_DTS AS LOAD_DTS_VIP,
        DENSE_RANK() OVER (
            PARTITION BY k.CUSTOMER_HK, d.AS_OF_DATE
            ORDER BY sv.EFFECTIVE_FROM DESC
        ) as rn_vip

    FROM snapshot_dates d
    CROSS JOIN all_keys k

    LEFT JOIN {{ ref('sat_customer_details') }} sd
        ON k.CUSTOMER_HK = sd.CUSTOMER_HK AND sd.EFFECTIVE_FROM <= d.AS_OF_DATE

    LEFT JOIN {{ ref('sat_customer_finance') }} sf
        ON k.CUSTOMER_HK = sf.CUSTOMER_HK AND sf.EFFECTIVE_FROM <= d.AS_OF_DATE

    LEFT JOIN {{ ref('sat_customer_vip') }} sv
        ON k.CUSTOMER_HK = sv.CUSTOMER_HK AND sv.EFFECTIVE_FROM <= d.AS_OF_DATE
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['CUSTOMER_HK', 'AS_OF_DATE']) }} AS PIT_HK,
    CUSTOMER_HK,
    AS_OF_DATE,

    -- Выбираем актуальные версии
    MAX(CASE WHEN rn_details = 1 THEN LOAD_DTS_DETAILS END) AS SAT_DETAILS_LOAD_DTS,
    MAX(CASE WHEN rn_finance = 1 THEN LOAD_DTS_FINANCE END) AS SAT_FINANCE_LOAD_DTS, -- ТЕПЕРЬ КОЛОНКА ЕСТЬ
    MAX(CASE WHEN rn_vip = 1 THEN LOAD_DTS_VIP END)         AS SAT_VIP_LOAD_DTS

FROM pit_logic
GROUP BY 1, 2, 3