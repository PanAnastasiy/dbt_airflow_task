{{ config(materialized='table') }}

SELECT
    pit.CUSTOMER_HK,
    pit.AS_OF_DATE as valid_from,
    LEAD(pit.AS_OF_DATE, 1, '9999-12-31') OVER (PARTITION BY pit.CUSTOMER_HK ORDER BY pit.AS_OF_DATE) as valid_to,

    h.customer_id,

    -- Данные из Sat Details (новые имена)
    sd.first_name,
    sd.phone,
    sd.address,
    sd.segment,

    -- Данные из Sat Finance (новые имена)
    sf.account_balance,

    COALESCE(sv.vip_status, 'Regular') as vip_status

FROM {{ ref('pit_customer') }} pit
INNER JOIN {{ ref('hub_customer') }} h
ON pit.CUSTOMER_HK = h.CUSTOMER_HK

    LEFT JOIN {{ ref('sat_customer_details') }} sd
    ON pit.CUSTOMER_HK = sd.CUSTOMER_HK
    AND pit.SAT_DETAILS_LOAD_DTS = sd.LOAD_DTS

    LEFT JOIN {{ ref('sat_customer_finance') }} sf
    ON pit.CUSTOMER_HK = sf.CUSTOMER_HK
    AND pit.SAT_FINANCE_LOAD_DTS = sf.LOAD_DTS

    LEFT JOIN {{ ref('sat_customer_vip') }} sv
    ON pit.CUSTOMER_HK = sv.CUSTOMER_HK
    AND pit.SAT_VIP_LOAD_DTS = sv.LOAD_DTS

WHERE pit.AS_OF_DATE <= CURRENT_DATE