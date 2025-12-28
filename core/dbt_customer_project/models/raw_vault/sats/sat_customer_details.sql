{{ config(materialized='incremental') }}

with source as (
    select
        customer_hk,
        customer_name,
        address,
        phone,
        market_segment,
        load_date,
        record_source,
        -- Генерируем Hash Diff для проверки изменений (CDC)
        {{ dbt_utils.generate_surrogate_key(['customer_name', 'address', 'phone', 'market_segment']) }} as hash_diff
    from {{ ref('stg_customers') }}
)

select * from source
    {% if is_incremental() %}
-- Грузим только если такой записи (с таким hash_diff) еще нет для этого ключа
where hash_diff not in (
    select hash_diff from {{ this }} where customer_hk = source.customer_hk
    )
    {% endif %}
