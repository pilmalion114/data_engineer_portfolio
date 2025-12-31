{{ config(
    materialized='table',
    schema='dbt_models'
) }}

-- 사용자 차원 테이블 (dbt 버전)
SELECT 
    user_id,
    username,
    age_group,
    region,
    created_at
FROM {{ source('movie_raw', 'dim_user') }}