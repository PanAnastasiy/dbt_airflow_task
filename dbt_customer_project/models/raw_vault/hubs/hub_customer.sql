{{ config(
    materialized='incremental',
    unique_key='CUSTOMER_PK'
) }}


SELECT DISTINCT
    CUSTOMER_PK,
    CUSTOMER_ID,
    LOAD_DATE,
    RECORD_SOURCE
FROM {{ ref('stg_customers') }}

    {% if is_incremental() %}
-- При инкрементальной загрузке добавляем только те PK, которых еще нет в таблице
WHERE CUSTOMER_PK NOT IN (SELECT CUSTOMER_PK FROM {{ this }})
    {% endif %}