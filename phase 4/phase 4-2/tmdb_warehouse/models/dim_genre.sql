{{ config(
    materialized='table',
    schema='dbt_models'
) }}

-- 장르 차원 테이블 (dbt 버전)
SELECT 
    genre_id,
    genre_name
FROM {{ source('movie_raw', 'dim_genre') }}