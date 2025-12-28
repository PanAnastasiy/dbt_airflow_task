{{ config(materialized='table') }}

with as_of_date as (
    select distinct load_date as snapshot_date
    from {{ ref('sat_customer_details') }}
),

sat_dates as (
    select
        customer_hk,
        load_date,
        lead(load_date, 1, '9999-12-31'::timestamp) over (partition by customer_hk order by load_date) as next_load_date
    from {{ ref('sat_customer_details') }}
)

select
    s.customer_hk,
    d.snapshot_date,
    s.load_date as sat_customer_details_ld
from as_of_date d
         inner join sat_dates s
                    on d.snapshot_date >= s.load_date
                        and d.snapshot_date < s.next_load_date