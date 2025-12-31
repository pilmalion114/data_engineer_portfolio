{{ config(
    materialized='table',
    schema='dbt_models'
) }}

-- 영화-장르 브릿지 테이블 (dbt 버전)
SELECT 
    movie_id,
    genre_id
FROM {{ source('movie_raw', 'movie_genre') }}