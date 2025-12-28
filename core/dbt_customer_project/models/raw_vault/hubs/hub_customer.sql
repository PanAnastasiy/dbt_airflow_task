{{ config(materialized='incremental') }}

with source as (
    select distinct
        customer_hk,
        customer_id,
        load_date,
        record_source
    from {{ ref('stg_customers') }}
)

select * from source
    {% if is_incremental() %}
where customer_hk not in (select customer_hk from {{ this }})
    {% endif %}