{{ config(materialized='table') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['c_nationkey']) }} AS state_pk
        , c_nationkey AS state_id
        , 'Nation #' || c_nationkey AS state_name
FROM {{ ref('stg_customers') }}
