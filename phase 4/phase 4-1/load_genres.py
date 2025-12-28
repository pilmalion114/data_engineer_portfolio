# 장르 데이터 적재 스크립트
# 목적: TMDB API에서 장르 목록을 가져와 dim_genre 테이블에 저장
# 순서: API 호출 -> JSON 파싱 -> DB INSERT
# 중요: Dimension 테이블은 Fact보다 먼저 채워야 함(외래키 참조)

import requests # HTTP 요청을 보내기 위한 라이브러리
import psycopg2 # PostgreSQL 연결 라이브러리
import os # 환경 변수 접근
from dotenv import load_dotenv # .env 파일 로드

load_dotenv() # .env 파일에서 환경 변수 읽어오기

# 1. TMDB API 설정
TMDB_TOKEN = os.getenv("TMDB_API_TOKEN") # .env에서 API 토큰 가져오기
GENRE_URL = "https://api.themoviedb.org/3/genre/movie/list" # 장르 목록 API 엔드포인트. cf.) 엔드포인트(endpoint): 어원 그대로, end(끝)+point(지점), 마지막 지점/부분이라고 이해해도 괜찮다. 공식적으로는 엔드포인트는 전체 URL이 맞지만, 개발자들 사이에서는 간단하게 공통된 url 부분은 버리고 마지막 부분을 엔드포인트라고 많이들 얘기한다.


# 2. API 요청 헤더 설정 (Bearer Token 인증 방식) 
headers = {
    "accept": "application/json", # 서버에게 응답을 JSON 형식으로 보내달라고 요청.(요청 헤더)
    "Authorization": f"Bearer {TMDB_TOKEN}" # Bearer 토큰으로 인증.
}


# 3. API 요청 파라미터 (한글 장르명 받기)
params = {
    "language": "Ko-KR" # 한국어로 장르명 받기
}

print("🎬 TMDB API에서 장르 목록 가져오는 중...")


# 4. API 호출
try:
    response = requests.get(GENRE_URL,headers=headers, params=params) # get 요청
    response.raise_for_status() # HTTP 에러 발생 시 예외(Exception) 발생 (4xx(클라이언트 오류),5xx(서버 오류)). 여기서 raise는 우리가 phase 2의 .ipynb 마지막 부분에서 배운 raise(에러날 시에 에러 위로 던지기)가 맞다. 
    data = response.json() # JSON 응답을 Python 딕셔너리로 변환.

    genres = data['genres'] # genres 키에서 장르 리스트 추출
    print(f"✅ {len(genres)}개 장르 가져오기 성공!")

    # 가져온 장르 미리보기 (처음 3개만)
    print("\n📋 가져온 장르 샘플:")
    for genre in genres[:3]:
        print(f"  - ID: {genre['id']}, 이름: {genre['name']}")

except Exception as e: # API 호출 실패 시
    print(f"❌ API 호출 실패: {e}")
    exit() # 프로그램 종료


# 5. PostgreSQL 연결
print("\n💾 PostgreSQL에 연결 중...")

conn = psycopg2.connect(
    host="localhost",
    port=int(os.getenv("POSTGRES_PORT")),
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

cursor = conn.cursor() # SQL 실행용 커서 객체 생성
print("✅ PostgreSQL 연결 성공!")


# 6. 기존 데이터 삭제 (개발 중에만 사용, 중복 방지)
print("\n🗑️  기존 장르 데이터 삭제 중...")

try:
    cursor.execute("delete from movie_genre;") # 외래키 참조하는 테이블 먼저 삭제(참조 무결성(정확하고 일관된 상태 유지 == 규칙/제약을 위반하지 않은 상태) 제약 위반 방지. 외래키 제약 에러 방지)
    cursor.execute("delete from dim_genre;") # dim_genre 데이터 삭제
    conn.commit() # 변경사항 저장(반영) - 이후에는 rollback 불가.
    print("✅ 기존 데이터 삭제 완료")
except Exception as e:
    print(f"❌ 삭제 실패: {e}")
    conn.rollback()  # 에러 발생 시 롤백


# 7. 장르 데이터 insert
print(f"\n📥 {len(genres)}개 장르 데이터 삽입 중...")

## insert 쿼리 (중복 시 무시: on conflict do nothing -> MySQL의 Upsert와 비슷(둘 다 중복 처리하는 메커니즘)하면서도 다름)
insert_query = """
    insert into dim_genre (genre_id, genre_name)
    values (%s, %s)
    on conflict (genre_id) do nothing;
"""

success_count = 0
fail_count = 0

for genre in genres:
    try:
        cursor.execute(insert_query, (
            genre['id'], 
            genre['name']
        ))
        success_count += 1

    except Exception as e:
        print(f"❌ 장르 삽입 실패 - ID: {genre['id']}, 이름: {genre['name']}, 에러: {e}")
        fail_count += 1

conn.commit() # 모든 insert 완료 후 한번에 커밋(트랜잭션 완료)
print(f"✅ 삽입 완료: 성공 {success_count}개, 실패 {fail_count}개")


# 8. 삽입 결과 확인
print("\n🔍 삽입된 데이터 확인 중...")

cursor.execute("select count(*) from dim_genre;")
count = cursor.fetchone()[0] # 결과의 첫 번째 값(갯수)
print(f"📊 dim_genre 테이블 총 레코드 수: {count}개")

## 실제 데이터 샘플 조회 (상위 5개)
cursor.execute("select genre_id, genre_name from dim_genre order by genre_id limit 5;")
samples = cursor.fetchall() # 모든 결과 가져오기

print("\n📋 저장된 장르 샘플 (5개):")
for genre_id, genre_name in samples:
    print(f"  - ID: {genre_id}, 이름: {genre_name}")


# 9. 리소스 정리
cursor.close()
conn.close()

print("\n🎉 장르 데이터 적재 완료!")
print(f"총 {count}개 장르가 dim_genre 테이블에 저장되었습니다.")



