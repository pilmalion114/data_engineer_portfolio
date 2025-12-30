{{ config(
    materialized='table',
    schema='dbt_models' 
    ) }} -- 저장 방식 설정(view,table,incremental). 처음에 view로 했다가 충돌이 일어나서, 이렇게 바꿈. 원본 테이블 데이터 유지하면서, dbt 모델도 만드는 방법.

-- 영화 차원 테이블 (dbt 버전)
SELECT 
    movie_id,
    title,
    original_title,
    release_date,
    popularity,
    vote_average,
    vote_count
FROM {{ source('movie_raw', 'dim_movie') }}