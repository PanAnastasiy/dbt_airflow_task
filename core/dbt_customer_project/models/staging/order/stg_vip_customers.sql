{{ config(materialized='view') }}

with source as (
    select * from {{ ref('vip_customers') }}
),

hashed as (
    select
        -- Генерируем такой же ключ, как в stg_customers, чтобы можно было сджойнить
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_hk,

        customer_id,
        vip_status,
        joined_vip_date,

        current_timestamp() as load_date,
        'MANUAL_CSV' as record_source
    from source
)

select * from hashed