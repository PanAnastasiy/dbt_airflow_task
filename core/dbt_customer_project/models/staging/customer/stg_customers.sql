{{ config(materialized='view') }}

with source as (
    select * from {{ source('snowflake_sample', 'customer') }}
),

hashed as (
    select
        -- Генерируем Hash Key для Хаба (Бизнес ключ - C_CUSTKEY)
        {{ dbt_utils.generate_surrogate_key(['c_custkey']) }} as customer_hk,

        -- Бизнес ключ
        c_custkey as customer_id,

        -- Атрибуты (Payload)
        c_name as customer_name,
        c_address as address,
        c_nationkey as nation_key,
        c_phone as phone,
        c_acctbal as account_balance,
        c_mktsegment as market_segment,

        -- Метаданные
        current_timestamp() as load_date,
        'SNOWFLAKE_SAMPLE' as record_source
    from source
)

select * from hashed