{{ config(
    materialized='table',
    schema='dbt_models'
) }}

-- 날짜 차원 테이블 (dbt 버전)
SELECT 
    date_id,
    year,
    month,
    day,
    quarter,
    day_of_week,
    day_name,
    is_weekend
FROM {{ source('movie_raw', 'dim_date') }}