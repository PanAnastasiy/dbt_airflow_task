{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

dim_cust as (
    select customer_id, vip_status from {{ ref('dim_customer') }}
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.total_price,
    o.order_status,
    o.priority,
    -- Подтягиваем данные из измерения для удобства аналитиков
    c.vip_status as customer_vip_status
from orders o
         left join dim_cust c on o.customer_id = c.customer_id