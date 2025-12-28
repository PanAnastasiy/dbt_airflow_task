{{ config(materialized='table') }}

with hub as (
    select customer_hk, customer_id
    from {{ ref('hub_customer') }}
),

bv_sat as (
    select * from {{ ref('sat_bv_customer_combined') }}
)

select
    h.customer_id,
    s.customer_name,
    s.market_segment,
    s.vip_status,
    s.joined_vip_date,
    s.load_date as last_updated
from hub h
         inner join bv_sat s on h.customer_hk = s.customer_hk