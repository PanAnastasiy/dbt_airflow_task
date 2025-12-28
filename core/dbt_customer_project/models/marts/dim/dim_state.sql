{{ config(materialized='table') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['c_nationkey']) }} as state_pk,
    c_nationkey as state_id,
    -- Предположим, логика маппинга или просто уникальный список
    'Nation #' || c_nationkey as state_name
FROM {{ ref('stg_customers') }}