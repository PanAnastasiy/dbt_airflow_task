{{ config(materialized='incremental') }}

with source as (
    select distinct
        link_customer_order_hk,
        order_hk,
        customer_hk,
        load_date,
        record_source
    from {{ ref('stg_orders') }}
)

select * from source
    {% if is_incremental() %}
where link_customer_order_hk not in (select link_customer_order_hk from {{ this }})
    {% endif %}
