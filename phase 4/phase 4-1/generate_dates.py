# 날짜 차원의 데이터 생성 스크립트
# 목적: dim_date 테이블에 2024-2025년 날짜 데이터 생성
# 방법: Python datetime으로 날짜 범위 생성 -> 각 날짜의 속성 계산 -> insert
# 특징: API 호출 없이 순수 계산으로 생성

import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta # 날짜 계산용

load_dotenv()

# 1. 날짜 범위 설정
START_DATE = datetime(2024,1,1) # 시작일
END_DATE = datetime(2025,12,31) # 종료일

print(f"📅 날짜 데이터 생성 중... ({START_DATE.date()} ~ {END_DATE.date()})")


# 2. 날짜 데이터 리스트 생성
dates_data = []  # 모든 날짜 데이터를 담을 리스트

current_date = START_DATE # 현재 처리 중인 날짜

while current_date <= END_DATE: # 종료일까지 반복
    ## 날짜 속성 계산
    date_id = current_date.date() # DATE 타입으로 변환 (YYYY-MM-DD)
    year = current_date.year  # 년도 (2024, 2025)
    month = current_date.month  # 월 (1~12)
    day = current_date.day  # 일 (1~31)

    ## 분기 계산 (1~4)
    ## 1~3월: 1분기, 4~6월: 2분기, 7~9월: 3분기, 10~12월: 4분기
    quarter = (month-1)//3 + 1 # 0을 3으로 나누는 건 수학적으로 가능하다. 어차피 0을 3으로 나눠도 똑같이 0이기 때문. 하지만, 다른 숫자를 0으로 나누는 것은 오류이다.(ZeroDivisionError). 그리고 //은 몫의 연산이다.

    ## 요일 번호 (0:월요일, 6:일요일)
    day_of_week = current_date.weekday() # 요일을 숫자로 나타내줌.

    ## 요일명(영어)
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_name = day_names[day_of_week] # 숫자(인덱스) 개념으로 매핑해서 요일명으로 나타냄.

    ## 주말 여부 (토요일:5, 일요일:6)
    is_weekend = day_of_week >= 5  # 5 이상이면 True(주말)

    ## 데이터 추가 -> 모든 날짜 정보 다 담기. 이 묶음이 하나의 튜플로 묶임.
    dates_data.append((
        date_id,
        year,
        month,
        day,
        quarter,
        day_of_week,
        day_name,
        is_weekend
    ))

    ## 다음 날로 이동 (+1일) -> timedelta 사용. delta의 의미는 변화량. 즉, 1만큼의 변화량을 붙인 의미임.
    current_date += timedelta(days=1)

print(f"✅ {len(dates_data)}개 날짜 데이터 생성 완료!")

## 샘플 확인 (처음 3개)
print("\n📋 생성된 날짜 샘플:")
for date_data  in dates_data[:3]:
    date_id, year, month, day, quarter, dow, day_name, is_weekend = date_data  # 각각의 date_data에 있는 값들을 파싱(parsing).
    weekend_str = "주말" if is_weekend else "평일"
    print(f"  - {date_id} ({day_name}, {weekend_str}) | {year}년 {quarter}분기")


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


# 4. 기존 데이터 삭제 (개발 중에만 사용)
print("\n🗑️  기존 dim_date 데이터 삭제 중...")

try:
    ## 외래키 참조하는 테이블 먼저 삭제
    cursor.execute("DELETE FROM fact_viewlog;")
    cursor.execute("DELETE FROM dim_date;")
    conn.commit()
    print("✅ 기존 데이터 삭제 완료")

except Exception as e:
    print(f"❌ 삭제 실패: {e}")
    conn.rollback()


# 5. 날짜 데이터 insert
print(f"\n📥 {len(dates_data)}개 날짜 데이터 삽입 중...")

## INSERT 쿼리
insert_query = """
    INSERT INTO dim_date (
        date_id, 
        year, 
        month, 
        day, 
        quarter, 
        day_of_week, 
        day_name, 
        is_weekend
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (date_id) DO NOTHING;
"""

try:
    ## executemany: 여러 개의 데이터를 한번에 insert(빠름!)
    ## executemany는 리스트의 각 튜플을 순회하며 insert 실행
    cursor.executemany(insert_query, dates_data)

    conn.commit()
    print(f"✅ {len(dates_data)}개 날짜 데이터 삽입 완료!")

except Exception as e:
    print(f"❌ 삽입 실패: {e}")
    conn.rollback()


# 6. 삽입 결과 확인
print("\n🔍 삽입된 데이터 확인 중...")

## 총 개수
cursor.execute("SELECT COUNT(*) FROM dim_date;")
count = cursor.fetchone()[0]
print(f"📊 dim_date 테이블 총 레코드 수: {count}개")

## 연도별 통계
cursor.execute("""
    select year, count(*) as day_count
    from dim_date
    group by year
    order by year;
""")
year_stats = cursor.fetchall()

print("\n📈 연도별 날짜 수:")
for year, day_count in year_stats:
    print(f"  - {year}년: {day_count}일")

## 주말/평일 통계
## case when: 조건에 따라 다른 값 반환(if문과 비슷)
cursor.execute("""
    select
        sum(case when is_weekend then 1 else 0 end) as weekend_count,
        sum(case when not is_weekend then 1 else 0 end) as weekday_count
    from dim_date;       
""")

weekend_count, weekday_count = cursor.fetchone() # 각각 sum 값이 한 행에 출력되므로 fetchall()이 아닌 fetchone() 사용.

print(f"\n📊 주말/평일 통계:")
print(f"  - 평일: {weekday_count}일")
print(f"  - 주말: {weekend_count}일")

## 샘플 데이터 조회 (2024년 첫 주)
cursor.execute("""
    SELECT date_id, day_name, is_weekend, quarter
    FROM dim_date
    WHERE year = 2024 AND month = 1
    ORDER BY date_id
    LIMIT 7;
""")
samples = cursor.fetchall()

print("\n📋 저장된 날짜 샘플 (2024년 1월 첫 주):")
for date_id, day_name, is_weekend, quarter in samples:
    weekend_str = "🌴 주말" if is_weekend else "💼 평일"
    print(f"  - {date_id} ({day_name}) | {quarter}분기 | {weekend_str}")


## 7. 리소스 정리
cursor.close()
conn.close()

print("\n🎉 날짜 데이터 생성 완료!")
print(f"총 {count}개 날짜가 dim_date 테이블에 저장되었습니다.")