{{ config(materialized='view') }}

with raw_sat as (
    select * from {{ ref('sat_customer_details') }}
    -- Берем самую свежую запись (упрощенно)
    qualify row_number() over (partition by customer_hk order by load_date desc) = 1
),

vip_data as (
    select * from {{ ref('stg_vip_customers') }}
)

select
    r.customer_hk,
    r.customer_name,
    r.market_segment,
    -- Бизнес логика: Если нет в файле, то статус Standard
    coalesce(v.vip_status, 'Standard') as vip_status,
    v.joined_vip_date,
    r.load_date
from raw_sat r
         left join vip_data v on r.customer_hk = v.customer_hk