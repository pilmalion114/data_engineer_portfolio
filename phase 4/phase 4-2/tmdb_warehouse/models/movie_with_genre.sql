-- ref() 함수를 연습하는 sql 모델링 코드입니다.

-- <목표> -- 
-- 1. 여러 dbt 모델을 JOIN
-- 2. ref() 함수로 의존성 자동 관리
-- 3. 집계 + 파생 컬럼 추가

{{ config(
    materialized='table',
    schema='dbt_models'
)}}

-- 영화 + 장르 통합 테이블 (ref 연습)
SELECT
    m.movie_id,
    m.title,
    m.release_date,
    m.vote_average,
    STRING_AGG(g.genre_name, ', ') as genres, -- strcat처럼 문자열 이어붙이기. ,로 구분한다.
    count(g.genre_id) as genre_count,
    case
        when m.vote_average >= 8.0 then 'Excellent'
        when m.vote_average >= 7.0 then 'Good'
        when m.vote_average >= 6.0 then 'Average'
        else 'poor'
    end as rating_category -- case-when-end 구조. end는 단순 case문의 끝(종료)를 나타낸다.
from {{ ref('dim_movie') }} m -- 간단히(단순히) 이해하자면, ref는 그냥 단어 의미 그대로, 다른 테이블을 '참조'하는 거라고 생각하면 됨. 다만 차이점은, 'FROM public_dbt_models.dim_movie m(일반 SQL)' 와 'ref'의 차이점은, 1. 테이블 참조(공통 부분) 2. 의존성 파악(ref) 3. 자동 순서 정렬(ref). ex.) 'movie_with_genre.sql'이 'dim_movie.sql'를 참조한다. -> dbt run하면, dim_movie 먼저 실행 -> 그 다음 movie_with_genre 실행. 
left join {{ ref('movie_genre') }} mg on m.movie_id = mg.movie_id -- left (outer) join: 왼쪽 테이블은 all, 오른쪽 테이블은 공통(겹치는) 부분만(없으면 Null). right (outer) join은 left join의 반대로. full (outer) join은 left+right(합집합). images/ 폴더에 있는 사진 참고. full outer join 시 공통 부분은 중복을 허용하여 겹치는 속성들이 중복으로 2개 나올 수 있고, 아니면 자연 조인(Natural Join, 겹치는 부분을 2개 다 열로 표현하는 게 아니라 하나만 사용하는 조인)으로 중복 제거해줄 수도 있다.
left join {{ ref('dim_genre') }} g on mg.genre_id = g.genre_id
group by m.movie_id, m.title, m.release_date, m.vote_average 
-- GROUP BY는 여러 컬럼 가능
-- 이 4개 컬럼 조합으로 그룹화 (실제로는 movie_id가 PK라 이것만으로 구분됨)
-- 핵심 이유: SQL 규칙 - SELECT의 비집계 컬럼은 전부 GROUP BY에 넣어야 함! -> 즉, String_AGG, count 외에 모든 열은 다 group by해야 함.
-- 안 그러면 에러 발생
