# 사용자 차원 데이터 생성 스크립트
# 목적: dim_user 테이블에 가상 사용자 데이터 생성
# 방법: Faker 라이브러리로 랜덤 사용자 정보 생성 -> insert
# 특징: 나중에 fact_viewlog에서 참조할 사용자들

import psycopg2
import os
from dotenv import load_dotenv
from faker import Faker # 가짜 데이터 생성 라이브러리

load_dotenv()

# 1. Faker 인스턴스 생성 (한국어)
fake = Faker('ko_KR') # 한국어 이름, 지역 생성


# 2. 사용자 데이터 생성 설정
NUM_USERS = 20 # 생성할 사용자 수 (10~50 사이 추천)

print(f"👥 {NUM_USERS}명의 가상 사용자 데이터 생성 중...")


## 연령대 옵션(랜덤 선택용)
age_groups = ['10대', '20대', '30대', '40대', '50대', '60대 이상']

## 지역 옵션(한국 주요 도시)
regions = ['서울', '부산', '대구', '인천', '광주', '대전', '울산', '세종', '경기', '강원', '충북', '충남', '전북', '전남', '경북', '경남', '제주']

users_data = [] # 모든 사용자 데이터를 담을 리스트

for i in range(NUM_USERS):
    ## Faker로 랜덤 데이터 생성
    username = fake.name() # 한글 이름
    age_group = fake.random_element(age_groups) # 연령대 랜덤 선택. 랜덤으로 리스트 안에 있는 값들 선택한다는 의미.
    region = fake.random_element(regions) # 지역 랜덤 선택

    ## 데이터 추가 (user_id는 SERIAL이라 자동 생성되므로 제외)
    users_data.append((
        username,
        age_group,
        region
    ))

print(f"✅ {len(users_data)}명 사용자 데이터 생성 완료!")

## 샘플 확인 (처음 5명)
print("\n📋 생성된 사용자 샘플:")
for idx, (username, age_group, region) in enumerate(users_data[:5], 1): # enumerate 객체
    print(f"  {idx}. {username} ({age_group}, {region})")


# 3. PostgreSQL 연결
print("\n💾 PostgreSQL에 연결 중...")

conn = psycopg2.connect(
    host="localhost",
    port=int(os.getenv("POSTGRES_PORT")),
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

cursor = conn.cursor()
print("✅ PostgreSQL 연결 성공!")


# 4. 기존 데이텨 삭제 (개발 중에만 사용)
print("\n🗑️  기존 dim_user 데이터 삭제 중...")

try:
    ## 외래키 참조하는 테이블 먼저 삭제
    cursor.execute("DELETE FROM fact_viewlog;")
    cursor.execute("DELETE FROM dim_user;")
    conn.commit()
    print("✅ 기존 데이터 삭제 완료")

except Exception as e:
    print(f"❌ 삭제 실패: {e}")
    conn.rollback()


# 5. 사용자 데이터 INSERT
print(f"\n📥 {len(users_data)}명 사용자 데이터 삽입 중...")

# INSERT 쿼리
insert_query = """
    INSERT INTO dim_user (username, age_group, region)
    VALUES (%s, %s, %s);
"""
# user_id는 SERIAL (자동 증가)라서 INSERT 시 제외

try:
    ## executemany: 여러 개 데이터를 한번에 INSERT
    cursor.executemany(insert_query, users_data)
    
    conn.commit()  # 모든 INSERT 완료 후 커밋
    print(f"✅ {len(users_data)}명 사용자 데이터 삽입 완료!")
    
except Exception as e:
    print(f"❌ 삽입 실패: {e}")
    conn.rollback()


# 6. 삽입 결과 확인
print("\n🔍 삽입된 데이터 확인 중...")

## 총 개수
cursor.execute("SELECT COUNT(*) FROM dim_user;")
count = cursor.fetchone()[0]
print(f"📊 dim_user 테이블 총 레코드 수: {count}명")

## 연령대별 통계
cursor.execute("""
    SELECT age_group, COUNT(*) as user_count
    FROM dim_user
    GROUP BY age_group
    ORDER BY age_group;
""")
age_stats = cursor.fetchall()

print("\n📈 연령대별 사용자 수:")
for age_group, user_count in age_stats:
    print(f"  - {age_group}: {user_count}명")

## 지역별 통계 (상위 5개)
cursor.execute("""
    SELECT region, COUNT(*) as user_count
    FROM dim_user
    GROUP BY region
    ORDER BY user_count DESC
    LIMIT 5;
""")
region_stats = cursor.fetchall()

print("\n📍 지역별 사용자 수 (TOP 5):")
for region, user_count in region_stats:
    print(f"  - {region}: {user_count}명")

## 전체 사용자 샘플 조회 (10명)
cursor.execute("""
    SELECT user_id, username, age_group, region
    FROM dim_user
    ORDER BY user_id
    LIMIT 10;
""")
samples = cursor.fetchall()

print("\n📋 저장된 사용자 샘플 (10명):")
for user_id, username, age_group, region in samples:
    print(f"  - [ID: {user_id}] {username} ({age_group}, {region})")


# 7. 리소스 정리
cursor.close()
conn.close()

print("\n🎉 사용자 데이터 생성 완료!")
print(f"총 {count}명의 사용자가 dim_user 테이블에 저장되었습니다.")