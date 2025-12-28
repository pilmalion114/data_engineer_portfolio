# 관람 기록 Fact 데이터 생성 스크립트
# 목적: fact_viewlog 테이블에 가상 관람 기록 생성
# 방법: dim_movie, dim_user, dim_date에서 랜덤 조합 -> 가상 평점/관람수 생성
# 특징: Fact 테이블(마지막 단계!)

import psycopg2
import os
from dotenv import load_dotenv
import random # 랜덤 선택용
from datetime import datetime # 날짜 처리

load_dotenv()

# 1. PostgreSQL 연결
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


# 2. Dimension 테이블에서 데이터 가져오기 (Fact 생성에 필요)
print("\n📊 Dimension 테이블에서 데이터 가져오는 중...")

## 영화 ID 목록
cursor.execute("select movie_id from dim_movie;")
movie_ids = [row[0] for row in cursor.fetchall()] # list comprehension(리스트 컴프리헨션): 각 row의 첫 번째 값만 추출 & 그리고 그것들을 리스트화.
print(f"✅ 영화 {len(movie_ids)}개 발견")

## 사용자 ID 목록
cursor.execute("SELECT user_id FROM dim_user;")
user_ids = [row[0] for row in cursor.fetchall()]
print(f"✅ 사용자 {len(user_ids)}명 발견")

## 날짜 목록(2024~2025년 중 랜덤 선택)
cursor.execute("SELECT date_id FROM dim_date WHERE year IN (2024, 2025);")
dates = [row[0] for row in cursor.fetchall()]
print(f"✅ 날짜 {len(dates)}개 발견")


# 3. 관람 기록 생성 설정
NUM_VIEWLOGS = 150 # 생성할 관람 기록 수 (100~300 추천)

print(f"\n🎬 {NUM_VIEWLOGS}개의 가상 관람 기록 생성 중...")

viewlogs_data = [] # 모든 관람 기록을 담을 리스트

for i in range(NUM_VIEWLOGS):
    ## 랜덤 조합 선택
    movie_id = random.choice(movie_ids) # 영화 랜덤 선택
    user_id = random.choice(user_ids)  # 사용자 랜덤 선택
    view_date = random.choice(dates)  # 날짜 랜덤 선택

    ## 가상 측정값 생성
    ## 평점: 1.0 ~ 10.0 (소수점 1자리)
    rating = round(random.uniform(1.0,10.0),1) # uniform: a,b 사이의 실수(소수점 있는 숫자)를 랜덤하게 반환. ,1은 소수점 1자리까지 표기를 의미.

    ## 관람 횟수: 1~3회 (대부분 1회)
    ## 80% 확률로 1회, 15% 확률로 2회, 5% 확률로 3회
    rand_val = random.random() # 0.0 ~ 1.0 랜덤 값
    if rand_val < 0.80: # 80%
        view_count = 1
    elif rand_val < 0.95:  # 15%
        view_count = 2
    else:  # 5%
        view_count = 3

    ## 데이터 추가 (view_id는 Serial이라 자동 생성). # Serial은 데이터베이스의 인덱스 AUTO_INCREMENT와 같은 역할.
    ## 이 묶음이 하나의 튜플로 묶임.
    viewlogs_data.append((
        movie_id,
        user_id,
        view_date,
        rating,
        view_count
    ))

print(f"✅ {len(viewlogs_data)}개 관람 기록 생성 완료!")

## 샘플 확인 (처음 5개)
print("\n📋 생성된 관람 기록 샘플:")
for idx, (movie_id, user_id, view_date, rating, view_count) in enumerate(viewlogs_data[:5], 1):
    print(f"  {idx}. 영화ID:{movie_id}, 사용자ID:{user_id}, 날짜:{view_date}, 평점:{rating}, 관람:{view_count}회")


# 4. 기존 데이터 삭제 (개발 중에만 사용)
print("\n🗑️  기존 fact_viewlog 데이터 삭제 중...")
try:
    cursor.execute("DELETE FROM fact_viewlog;")
    conn.commit()
    print("✅ 기존 데이터 삭제 완료")
except Exception as e:
    print(f"❌ 삭제 실패: {e}")
    conn.rollback()


# 5. 관람 기록 insert
print(f"\n📥 {len(viewlogs_data)}개 관람 기록 삽입 중...")

## INSERT 쿼리
insert_query = """
    INSERT INTO fact_viewlog (
        movie_id, 
        user_id, 
        view_date, 
        rating, 
        view_count
    )
    VALUES (%s, %s, %s, %s, %s);
"""
## view_id는 SERIAL (자동 증가)

try:
    ## executemany: 여러 개 데이터를 한번에 insert
    cursor.executemany(insert_query,viewlogs_data)

    conn.commit() # 모든 insert 완료 후 커밋(트랜잭션 단위)
    print(f"✅ {len(viewlogs_data)}개 관람 기록 삽입 완료!")

except Exception as e:
    print(f"❌ 삽입 실패: {e}")
    conn.rollback()


# 6. 삽입 결과 확인
print("\n🔍 삽입된 데이터 확인 중...")

## 총 갯수
cursor.execute("select count(*) from fact_viewlog;")
count = cursor.fetchone()[0]
print(f"📊 fact_viewlog 테이블 총 레코드 수: {count}개")

## 평점 통계
cursor.execute("""
    select
        avg(rating) as avg_rating,
        min(rating) as min_rating,
        max(rating) as max_rating
    from fact_viewlog;
""")

avg_rating,min_rating,max_rating = cursor.fetchone()
print(f"\n⭐ 평점 통계:")
print(f"  - 평균: {float(avg_rating):.2f}")
print(f"  - 최저: {float(min_rating):.1f}")
print(f"  - 최고: {float(max_rating):.1f}")

## 관람수별 분포
cursor.execute("""
    select view_count, count(*) as record_count
    from fact_viewlog
    group by view_count
    order by view_count;
""")

view_count_stats = cursor.fetchall()

print(f"\n📊 관람 횟수 분포:")
for view_count, record_count in view_count_stats:
    percentage = (record_count / count) * 100 # count는 위에 보면 변수로 선언함.
    print(f"  - {view_count}회: {record_count}건 ({percentage:.1f}%)")

## 가장 많이 본 영화 TOP 5
cursor.execute("""
    SELECT 
        m.title,
        COUNT(*) as view_count,
        AVG(f.rating) as avg_rating
    FROM fact_viewlog f
    JOIN dim_movie m ON f.movie_id = m.movie_id
    GROUP BY m.title
    ORDER BY view_count DESC
    LIMIT 5;
""")
top_movies = cursor.fetchall()

print(f"\n🎬 가장 많이 본 영화 TOP 5:")
for idx, (title, view_count, avg_rating) in enumerate(top_movies, 1):
    print(f"  {idx}. {title} - {view_count}회 시청, 평균 평점 {float(avg_rating):.2f}")

## 샘플 데이터 조회 (실제 정보와 함께)
cursor.execute("""
    SELECT 
        f.view_id,
        m.title,
        u.username,
        f.view_date,
        f.rating,
        f.view_count
    FROM fact_viewlog f
    JOIN dim_movie m ON f.movie_id = m.movie_id
    JOIN dim_user u ON f.user_id = u.user_id
    ORDER BY f.view_id
    LIMIT 10;
""")
samples = cursor.fetchall()

print(f"\n📋 저장된 관람 기록 샘플 (10개):")
for view_id, title, username, view_date, rating, view_count in samples:
    print(f"  [{view_id}] {username}님이 '{title}'를 {view_date}에 평점 {rating}로 {view_count}회 시청")


# 7. 리소스 정리
cursor.close()
conn.close()

print("\n🎉 관람 기록 데이터 생성 완료!")
print(f"총 {count}개의 관람 기록이 fact_viewlog 테이블에 저장되었습니다.")