{{ config(
    materialized='incremental',
    schema='dbt_models',
    unique_key='view_id'  
)}}
-- unique_key: 중복 데이터 발생 시, 중복 데이터 처리 기준. unique_key가 중복되는 새 데이터가 들어오면, 덮어쓰는(update) 방식을 택한다.


-- 조회 로그 fact 테이블 (dbt 버전 - incremental)
SELECT
    view_id,
    movie_id,
    user_id,
    view_date,
    rating,
    view_count,
    created_at
FROM {{ source('movie_raw', 'fact_viewlog') }}

{% if is_incremental() %}
    -- 이미 로드된 데이터 이후의 것만 추가(기존의 것은 냅두고, 새로 들어온 데이터만 추가)
    where created_at > (SELECT max(created_at) from {{this}}) -- {{this}}: 현재 이 테이블 자기 자신.
{% endif %} -- 단순한 끝 표시. 그냥 if절이 끝났다라고 이해하면 됨.

-- 첫 실행 --
-- is_incremental() = False
-- → 전체 데이터 로드

--**두 번째 실행부터:**
--
-- is_incremental() = True
-- → 마지막 created_at 이후 데이터만 추가
-- → 효율성 UP! ⚡