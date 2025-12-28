# 영화 데이터 적재 스크립트
# 목적: TMDB API에서 인기 영화 목록을 가져와 dim_movie 테이블에 저장
# 순서: API 호출 -> JSON 파싱 -> DB INSERT
# 특징: 여러 페이지의 영화 데이터를 가져올 수 있음.

import requests
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime # 날짜 파싱용

load_dotenv()

# 1. TMDB API 설정
TMDB_TOKEN = os.getenv("TMDB_API_TOKEN")
MOVIE_URL = "https://api.themoviedb.org/3/movie/popular"


# 2. API 요청 헤더 (Bearer Token 인증)
headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {TMDB_TOKEN}"
}


# 3. 가져올 페이지 수 설정
PAGES = 3  # 1페이지당 약 20개, 3페이지 = 약 60개 영화
# 필요하면 숫자 조정 가능 (1-5 정도 추천)

print(f"🎬 TMDB API에서 인기 영화 데이터 가져오는 중... (총 {PAGES}페이지)")

all_movies = []  # 모든 영화 데이터를 담을 리스트


# 4. 여러 페이지의 데이터 가져오기
for page in range(1, PAGES+1):
    params = {
        "language": "ko-KR",  # 한국어 제목
        "page": page  # 페이지 번호
    }

    try:
        response = requests.get(MOVIE_URL, headers=headers, params=params)
        response.raise_for_status() # HTTP 에러 체크
        data=response.json() # JSON 응답을 Python 딕셔너리로 변환.

        movies = data['results'] # 'results'에는 각 영화의 여러 개의 데이터들이 있음. 
        all_movies.extend(movies) # append() vs extend() -> append()는 인자를 '하나의 요소'로 묶어서 추가. extend()는 리스트의 각 요소들을 개별적으로(분리해서, 안 묶어서) 추가.

        print(f"✅ {page}페이지: {len(movies)}개 영화 가져오기 성공")

    except Exception as e:
        print(f"❌ {page}페이지 가져오기 실패: {e}")
        continue # 실패해도 다음 페이지 계속 시도

print(f"✅ 총 {len(all_movies)}개 영화 데이터 수집 완료!")

## 영화 샘플 미리보기 (상위 3개)
print("\n📋 가져온 영화 샘플:")
for movie in all_movies[:3]:
    print(f"  - ID: {movie['id']}, 제목: {movie['title']}, 평점: {movie['vote_average']}")


# 5. PostgreSQL 연결
print("\n💾 PostgreSQL에 연결 중...")

conn = psycopg2.connect(
    host="localhost",
    port=int(os.getenv("POSTGRES_PORT")),
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

cursor =conn.cursor()
print("✅ PostgreSQL 연결 성공!")


# 6. 기존 데이터 삭제(개발 중에만 사용)
print("\n🗑️  기존 영화 데이터 삭제 중...")

try:
    # 외래키를 참조하는 테이블들 먼저 삭제
    cursor.execute("delete from fact_viewlog;")
    cursor.execute("delete from movie_genre;")
    cursor.execute("delete from dim_movie;")

    conn.commit()

    print("✅ 기존 데이터 삭제 완료")

except Exception as e:
    print(f"❌ 삭제 실패: {e}")
    conn.rollback()


# 7. 영화 데이터 insert
print(f"\n📥 {len(all_movies)}개 영화 데이터 삽입 중...")

## insert 쿼리 (중복 시 무시)
insert_query = """
    insert into dim_movie (
        movie_id,
        title,
        original_title, 
        release_date, 
        overview, 
        popularity, 
        vote_average, 
        vote_count, 
        adult
    )
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (movie_id) DO NOTHING;
"""
# ON CONFLICT: 같은 movie_id가 이미 있으면 무시

success_count = 0
fail_count = 0

for movie in all_movies:
    try:
        ## release_date 파싱 (문자열 -> DATE). ※parsing: 구분하다 -> 분석하다 -> 변환하다.
        ## TMDB는 'YYYY-MM-DD' 형식 또는 빈 문자열
        release_date = movie.get("release_date")
        if release_date:
            release_date = datetime.strptime(release_date,'%Y-%m-%d').date() # strptime: 문자열을 날짜 객체로 변환. # .date(): datetime에서 date 부분만 추출.
        else:
            release_date = None # Null로 저장.

        ## insert 실행
        cursor.execute(insert_query, (
            movie['id'],                      # movie_id (TMDB ID)
            movie['title'],                   # title (한글 제목)
            movie['original_title'],          # original_title (원제)
            release_date,                     # release_date (DATE 타입)
            movie.get('overview', ''),        # overview (줄거리, 없으면 빈 문자열)
            movie.get('popularity', 0),       # popularity (인기도, 없으면 0)
            movie.get('vote_average', 0),     # vote_average (평균 평점)
            movie.get('vote_count', 0),       # vote_count (투표 수)
            movie.get('adult', False)         # adult (성인 등급 여부)
        ))
        success_count += 1
    
    except Exception as e:
        print(f"❌ 영화 삽입 실패 - ID: {movie['id']}, 제목: {movie['title']}, 에러: {e}")
        fail_count += 1

conn.commit() # 모든 insert 완료 후 커밋
print(f"✅ 삽입 완료: 성공 {success_count}개, 실패 {fail_count}개")


# 8. 삽입 결과 확인
print("\n🔍 삽입된 데이터 확인 중...")

## 총 갯수
cursor.execute("select count(*) from dim_movie;")
count = cursor.fetchone()[0]
print(f"📊 dim_movie 테이블 총 레코드 수: {count}개")

## 실제 데이터 샘플 조회 (인기도 높은 순 5개)
cursor.execute("""
    select movie_id, title, release_date, vote_average, popularity
    from dim_movie
    order by popularity desc
    limit 5;
""")

samples = cursor.fetchall()

print("\n📋 저장된 영화 샘플 (인기 순위 TOP 5):")
for movie_id, title, release_date, vote_avg, popularity in samples:
    print(f"  - [{movie_id}] {title} | 개봉: {release_date} | 평점: {vote_avg} | 인기도: {popularity:.1f}")


# 9. 리소스 정리
cursor.close()
conn.close()

print("\n🎉 영화 데이터 적재 완료!")
print(f"총 {count}개 영화가 dim_movie 테이블에 저장되었습니다.")




