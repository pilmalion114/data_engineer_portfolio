# PostgreSQL에 테이블 생성 스크립트
# 목적: PostgreSQL에 Star Schema 테이블 구조 생성
# 순서: Dimension 테이블 먼저 -> Bridge 테이블 -> Fact 테이블 마지막
# 이유: 외래키 관계 때문에 참조되는 테이블이 먼저 존재해야 함.

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

## 1. PostgreSQL 데이터베이스 연결
conn = psycopg2.connect(
    host="localhost",  # 접속할 서버 주소 (로컬)
    port=int(os.getenv("POSTGRES_PORT")),  # 포트 번호 (.env에서 읽기)
    database=os.getenv("POSTGRES_DB"),  # 데이터베이스 이름
    user=os.getenv("POSTGRES_USER"),  # 사용자 이름
    password=os.getenv("POSTGRES_PASSWORD")  # 비밀번호
)

## 2. 커서 생성
cursor = conn.cursor()

print("🚀 테이블 생성 시작...")

## 3. 기존 테이블 삭제(개발 중 재실행 시 필요함)
# CASCADE: 의존 관계에 있는 모든 객체(외래키 등)도 함께 삭제. 연쇄 삭제(처리)라는 뜻을 가짐.
drop_tables = """
drop table if exists fact_viewlog cascade;
drop table if exists movie_genre cascade;
drop table if exists dim_movie cascade;
drop table if exists dim_genre cascade;
drop table if exists dim_date cascade;
drop table if exists dim_user cascade;
"""

try:
    cursor.execute(drop_tables)
    conn.commit()
    print("✅ 기존 테이블 삭제 완료")
except Exception as e:
    print(f"❌ 테이블 삭제 실패: {e}")
    conn.rollback() # 변경사항 취소.(원래 상태로 복구) # commit 취소는 아님. commit은 변경사항 적용까지이므로, commit이 되면 rollback이 불가능함. 즉, rollback은 마지막 commit 이후의 변경사항 취소임(마지막 commit 시점으로 복구).



## 4. Dimension 테이블 생성(Fact 테이블보다 먼저 생성해야 외래키 참조 가능)

# 4-1. dim_movie: 영화 기본 정보 저장
create_dim_movie = """
create table dim_movie (
    movie_id integer primary key, -- TMDB 영화 ID (기본키)
    title varchar(255) not null, -- 영화 제목 (필수)
    original_title varchar(255), -- 원제
    release_date date, -- 개봉일
    overview text, -- 줄거리 (긴 텍스트)
    popularity decimal(10,3), -- 인기도 (소수점 3자리)
    vote_average decimal(3,1), -- 평균 평점 (0.0 ~ 10.0)
    vote_count integer, -- 투표 수
    adult boolean, -- 성인 등급 여부 (TRUE/FALSE)
    created_at timestamp default now() -- 레코드 생성 시간 (자동 입력)
);
"""

try:
    cursor.execute(create_dim_movie)
    conn.commit()
    print("✅ dim_movie 테이블 생성 완료")
except Exception as e:
    print(f"❌ dim_movie 생성 실패: {e}")
    conn.rollback()

# 4-2. dim_genre: 장르 마스터 테이블
create_dim_genre = """
CREATE TABLE dim_genre (
    genre_id INTEGER PRIMARY KEY,           -- TMDB 장르 ID (기본키)
    genre_name VARCHAR(50) NOT NULL,        -- 장르명 (예: Action, Drama)
    created_at TIMESTAMP DEFAULT NOW()      -- 레코드 생성 시간
);
"""

try:
    cursor.execute(create_dim_genre)
    conn.commit()
    print("✅ dim_genre 테이블 생성 완료")
except Exception as e:
    print(f"❌ dim_genre 생성 실패: {e}")
    conn.rollback()

# 4-3. movie_genre: 영화-장르 '다대다' 관계 해결(Bridge Table)
# 한 영화는 여러 장르를 가질 수 있고, 한 장르는 여러 영화에 속할 수 있다. -> 다대다(n:m)
create_movie_genre = """
create table movie_genre (
    movie_id integer not null, -- 영화 ID (외래키)
    genre_id integer not null, -- 장르 ID (외래키)
    primary key (movie_id,genre_id), -- 복합키(둘 이상의 조합으로 이루어진 기본키): 같은 조합 중복 방지
    foreign key (movie_id) references dim_movie(movie_id), -- dim_movie 참조
    foreign key (genre_id) references dim_genre(genre_id) -- dim_genre 참조
);
"""

try:
    cursor.execute(create_movie_genre)
    conn.commit()
    print("✅ movie_genre 테이블 생성 완료")
except Exception as e:
    print(f"❌ movie_genre 생성 실패: {e}")
    conn.rollback()

# 4-4. dim_date: 날짜 차원 테이블(시계열 분석용)
create_dim_date = """
create table dim_date (
    date_id date primary key, -- 날짜 (기본키, YYYY-MM-DD)
    year integer not null, -- 년 (2025)
    month integer not null, -- 월 (1~12)
    day integer not null, -- 일 (1~31)
    quarter integer not null, -- 분기 (1~4)
    day_of_week integer not null, -- 요일 번호 (0=월, 6=일)
    day_name varchar(10) not null, -- 요일명 (Monday, Tuesday...)
    is_weekend boolean not null, -- 주말 여부 (토/일 = TRUE)
    created_at timestamp default now()
);
"""

try:
    cursor.execute(create_dim_date)
    conn.commit()
    print("✅ dim_date 테이블 생성 완료")
except Exception as e:
    print(f"❌ dim_date 생성 실패: {e}")
    conn.rollback()

# 4-5. dim_user: 사용자 정보(가상으로 생성할 예정)
create_dim_user = """
create table dim_user (
    user_id serial primary key, -- 사용자 ID (자동 증가). auto_increment(mySQL)와의 차이점: 둘 다 의미상 자동 증가 숫자이다. 근데 postgresql의 serial은 약간의 디테일이 있는데, sequence라는 객체를 만들어서 '시작값','증가값','최댓값 도달 시 에러(재시작 안 함)','다음 번호 확인','현재 번호 확인','시작 번호 변경' 같은 게 가능하다.
    username varchar(50) not null, -- 사용자명
    age_group varchar(20), -- 연령대 (10대, 20대...)
    region varchar(50), -- 지역
    created_at timestamp default now()
);
"""

try:
    cursor.execute(create_dim_user)
    conn.commit()
    print("✅ dim_user 테이블 생성 완료")
except Exception as e:
    print(f"❌ dim_user 생성 실패: {e}")
    conn.rollback()

# 5. Fact 테이블 생성(마지막! 모든 Dimension이 존재해야 한다.)
# fact_viewlog: 영화 관람 기록 (측정 데이터)
create_fact_viewlog = """
create table fact_viewlog (
    view_id serial primary key, -- 관람 기록 ID (자동 증가)
    movie_id integer not null, -- 영화 ID (외래키 → dim_movie)
    user_id integer not null, -- 사용자 ID (외래키 → dim_user)
    view_date date not null, -- 관람 날짜 (외래키 → dim_date)

    -- Measures (측정값 - 집계/분석 대상)
    rating decimal(3,1), -- 평점 (0.0 ~ 10.0)
    view_count integer default 1, -- 관람 횟수

    created_at timestamp default now(), -- 레코드 생성 시간

    -- 외래키 제약조건 설정
    foreign key (movie_id) references dim_movie(movie_id),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
    FOREIGN KEY (view_date) REFERENCES dim_date(date_id)
);
"""

try:
    cursor.execute(create_fact_viewlog)
    conn.commit()
    print("✅ fact_viewlog 테이블 생성 완료")
except Exception as e:
    print(f"❌ fact_viewlog 생성 실패: {e}")
    conn.rollback()


# 6. 생성된 테이블 목록 확인
# information_schema: PostgreSQL 시스템 카탈로그(메타데이터 저장)
cursor.execute("""
    select table_name
    from information_schema.tables
    where table_schema = 'public'
    order by table_name;
""")

tables = cursor.fetchall() # 모든 결과 가져오기(리스트 형태)

print("\n📊 생성된 테이블 목록:")
for table in tables:  # 각 테이블 이름 출력
    print(f"  - {table[0]}")

# 7. 리소스 정리(메모리 해제)
cursor.close() # 커서 닫기
conn.close() # 데이터베이스 연결 닫기

print("\n🎉 테이블 생성 완료!")