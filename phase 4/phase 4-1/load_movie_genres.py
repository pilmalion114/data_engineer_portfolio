# 영화-장르 관계 데이터 적재 스크립트
# 목적: dim_movie에 저장된 영화들의 장르 관계를 movie_genre 테이블에 저장
# 방법: TMDB API로 각 영화의 상세 정보를 조회하여 genre_ids 추출
# Bridge Table: 영화와 장르의 다대다(N:M) 관계 해결

import requests
import psycopg2
import os
from dotenv import load_dotenv
import time # API 호출 간격 조절용

load_dotenv()

# 1. TMDB API 설정
TMDB_TOKEN = os.getenv("TMDB_API_TOKEN")


# 2. API 요청 헤더
headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {TMDB_TOKEN}"
}


# 3. PostgreSQL 연결
print("💾 PostgreSQL에 연결 중...")

conn = psycopg2.connect(
    host="localhost",
    port=int(os.getenv("POSTGRES_PORT")),
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

cursor = conn.cursor()
print("✅ PostgreSQL 연결 성공!")


# 4. dim_movie에서 모든 영화 ID 가져오기
print("\n🎬 dim_movie에서 영화 ID 목록 가져오는 중...")

cursor.execute("select movie_id, title from dim_movie order by movie_id;")
movies = cursor.fetchall() # [(movie_id, title), ...] 형태

print(f"✅ {len(movies)}개 영화 발견!")


# 5. 기존 movie_genre 데이터 삭제 (중복 방지)
print("\n🗑️  기존 movie_genre 데이터 삭제 중...")

try:
    cursor.execute("delete from movie_genre;")
    conn.commit()
    print("✅ 기존 데이터 삭제 완료")

except Exception as e:
    print(f"❌ 삭제 실패: {e}")
    conn.rollback()


# 6. 각 영화의 장르 정보 가져와서 저장
print(f"\n🔗 {len(movies)}개 영화의 장르 관계 저장 중...")

## insert 쿼리
insert_query = """
    insert into movie_genre (movie_id, genre_id)
    values (%s, %s)
    on conflict (movie_id, genre_id) do nothing;
"""

success_count = 0  # 성공한 영화 수
total_relations = 0  # 저장된 관계 총 개수
fail_count = 0  # 실패한 영화 수

for idx, (movie_id, title) in enumerate(movies,1): # enumerate: 인덱스와 값 동시 반환
    try:
        ## TMDB API로 영화 상세 정보 조회
        movie_detail_url = f"https://api.themoviedb.org/3/movie/{movie_id}"
        params = {"language": "ko-KR"}

        response = requests.get(movie_detail_url, headers=headers, params=params)
        response.raise_for_status() # HTTP 에러 체크

        movie_data = response.json() # JSON 응답을 Python 딕셔너리로 변환.
        genres = movie_data.get('genres', []) # 'genres' 리스트 가져오기(없으면 빈 리스트([]) 반환.)
        ## genres 형태: [{'id': 28, 'name': 'Action'}, {'id': 53, 'name': 'Thriller'}, ...]

        ## 각 장르마다 movie_genre에 insert
        for genre in genres:
            genre_id = genre['id']
            cursor.execute(insert_query, (movie_id,genre_id))
            total_relations += 1

        success_count += 1

        ## 진행 상황 출력 (10개 마다)
        if idx % 10 == 0:
            print(f"  진행 중... {idx}/{len(movies)} ({idx/len(movies)*100:.1f}%)")

        ## API 호출 제한 방지 (0.1초 대기)
        time.sleep(0.1) # 1초에 10번 호출 (TMDB 제한: 초당 40번)
    
    except Exception as e:
        print(f"❌ 영화 처리 실패 - ID: {movie_id}, 제목: {title}, 에러: {e}")
        fail_count += 1

conn.commit()
print(f"\n✅ 처리 완료: 성공 {success_count}개, 실패 {fail_count}개")
print(f"📊 총 {total_relations}개의 영화-장르 관계 저장됨")


# 7. 저장 결과 확인
print("\n🔍 저장된 데이터 확인 중...")

## movie_genre 총 갯수
cursor.execute("select count(*) from movie_genre;")
count = cursor.fetchone()[0]
print(f"📊 movie_genre 테이블 총 레코드 수: {count}개")

## 샘플 데이터 조회 (영화와 장르 이름 함께) - 공통 내부 조인(Inner Join) - 교집합
cursor.execute("""
    select 
        m.movie_id,
        m.title,
        g.genre_name
    from movie_genre mg 
    join dim_movie m on mg.movie_id = m.movie_id
    join dim_genre g on mg.genre_id = g.genre_id -- 내부 조인이 2번 일어남.
    order by m.movie_id
    limit 10;  
""")
samples = cursor.fetchall()

print("\n📋 저장된 영화-장르 관계 샘플 (10개):")
current_movie = None # 현재 출력 중인 영화

for movie_id,title,genre_name in samples:
    if movie_id != current_movie: # 새로운 영화면
        print(f"\n  [{movie_id}] {title}")
        current_movie = movie_id
    print(f"    → {genre_name}")


# 8. 통계 확인
print("\n📈 통계:")

## 영화 당 평균 장르 수 - 서브쿼리 활용
cursor.execute("""
    select avg(genre_count) as avg_genres
    from (
        select movie_id, count(*) as genre_count
        from movie_genre
        group by movie_id
    ) sub;
""")

## 위 쿼리 실행 구체화
# """
# -- 원본 데이터 (movie_genre)
# movie_id | genre_id
# ---------|----------
#   798645 |       28
#   798645 |       53
#   798645 |      878
#  1084242 |       16
#  1084242 |       35

# -- ↓ 서브쿼리 실행 (GROUP BY + COUNT)

# movie_id | genre_count
# ---------|------------
#   798645 |           3  ← COUNT(*)
#  1084242 |           2  ← COUNT(*)

# """

avg_genres = cursor.fetchone()[0]
print(f"  - 영화당 평균 장르 수: {float(avg_genres):.2f}개")

## 장르별 영화 수
cursor.execute("""
    select g.genre_name, count(*) as movie_count
    from movie_genre mg
    join dim_genre g on mg.genre_id = g.genre_id
    group by g.genre_name
    order by movie_count desc
    limit 5;
""")

## 위 쿼리 실행 구체화
# """
# (movie_genre 테이블(Bridge Table))
# movie_id | genre_id
# ---------|----------
#   798645 |       28  (Action)
#   798645 |       53  (Thriller)
#   798645 |      878  (Sci-Fi)
#  1084242 |       16  (Animation)
#  1084242 |       35  (Comedy)
#  1084242 |       12  (Adventure)
#  1223601 |       28  (Action)
#  1223601 |       53  (Thriller)
#      425 |       28  (Action)
#      425 |       18  (Drama)

     
# (dim_genre 테이블)
# genre_id | genre_name
# ---------|-------------------
#       28 | Action
#       53 | Thriller
#      878 | Science Fiction
#       16 | Animation
#       35 | Comedy
#       12 | Adventure
#       18 | Drama

# **결과 (교집합):**
# ```
# mg.movie_id | mg.genre_id | g.genre_id | g.genre_name
# ------------|-------------|------------|-------------------
#      798645 |          28 |         28 | Action
#      798645 |          53 |         53 | Thriller
#      798645 |         878 |        878 | Science Fiction
#     1084242 |          16 |         16 | Animation
#     1084242 |          35 |         35 | Comedy
#     1084242 |          12 |         12 | Adventure
#     1223601 |          28 |         28 | Action
#     1223601 |          53 |         53 | Thriller
#         425 |          28 |         28 | Action
#         425 |          18 |         18 | Drama

# **장르별로 그룹화 (같은 genre_name끼리 묶음)**
# ↓

# [Action 그룹]
#   798645 | 28 | Action
#  1223601 | 28 | Action  
#      425 | 28 | Action
#  → COUNT(*) = 3

# [Thriller 그룹]
#   798645 | 53 | Thriller
#  1223601 | 53 | Thriller
#  → COUNT(*) = 2

# [Science Fiction 그룹]
#   798645 | 878 | Science Fiction
#  → COUNT(*) = 1

# [Animation 그룹]
#  1084242 | 16 | Animation
#  → COUNT(*) = 1

# [Comedy 그룹]
#  1084242 | 35 | Comedy
#  → COUNT(*) = 1

# [Adventure 그룹]
#  1084242 | 12 | Adventure
#  → COUNT(*) = 1

# [Drama 그룹]
#      425 | 18 | Drama
#  → COUNT(*) = 1
# ```

# **결과:**
# ```
# genre_name       | movie_count
# -----------------|------------
# Action           |           3
# Thriller         |           2
# Science Fiction  |           1
# Animation        |           1
# Comedy           |           1
# Adventure        |           1
# Drama            |           1
# """

top_genres = cursor.fetchall()
print("\n  - 가장 많은 영화가 속한 장르 TOP 5:")
for genre_name, movie_count in top_genres:
    print(f"    {genre_name}: {movie_count}편")


# 9. 리소스 정리
cursor.close()
conn.close()

print("\n🎉 영화-장르 관계 데이터 적재 완료!")
print(f"총 {count}개의 관계가 movie_genre 테이블에 저장되었습니다.")


