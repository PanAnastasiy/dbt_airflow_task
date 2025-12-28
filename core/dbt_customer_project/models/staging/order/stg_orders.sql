{{ config(materialized='view') }}

with source as (
    select * from {{ source('snowflake_sample', 'orders') }}
),

hashed as (
    select
        -- Hash Key для Хаба Заказов
        {{ dbt_utils.generate_surrogate_key(['o_orderkey']) }} as order_hk,

        -- Foreign Hash Key для Линка (связь с Customer)
        {{ dbt_utils.generate_surrogate_key(['o_custkey']) }} as customer_hk,

        -- Hash Key для самого Линка
        {{ dbt_utils.generate_surrogate_key(['o_orderkey', 'o_custkey']) }} as link_customer_order_hk,

        -- Бизнес ключи
        o_orderkey as order_id,
        o_custkey as customer_id,

        -- Атрибуты
        o_orderstatus as order_status,
        o_totalprice as total_price,
        o_orderdate as order_date,
        o_orderpriority as priority,

        -- Метаданные
        current_timestamp() as load_date,
        'SNOWFLAKE_SAMPLE' as record_source
    from source
)

select * from hashed