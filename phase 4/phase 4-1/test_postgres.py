# PostgreSQL 연결 테스트 코드
# 목적: Docker로 실행한 PostgreSQL 컨테이너에 python으로 연결 가능한지 확인.

import psycopg2 # PostgreSQL 데이터베이스 연결을 위한 라이브러리
import os # 운영체제 환경 변수 접근용(os.getenv())
from dotenv import load_dotenv # .env 파일의 환경 변수를 로드하는 라이브러리

load_dotenv() # .env 파일 로드(현재 디렉토리 및 상위 디렉토리에서 .env 탐색)

# 1. PostgreSQL 데이터베이스 연결 객체 생성
conn = psycopg2.connect(
    host="localhost", # 접속할 서버 주소(로컬 컴퓨터)
    port=int(os.getenv("POSTGRES_PORT")), # 포트 번호(.env에서 읽어옴, 문자열 -> 정수 변환)
    database=os.getenv("POSTGRES_DB"), # 접속할 데이터베이스 이름
    user=os.getenv("POSTGRES_USER"), # 사용자 이름
    password=os.getenv("POSTGRES_PASSWORD") # 비밀번호
)

print("✅ PostgreSQL 연결 성공!")

cursor = conn.cursor() # 커서 객체 생성

cursor.execute("select version();") # 커서로 PostgreSQL 버전 정보 조회

version = cursor.fetchone() # 위 명령어 실행한 '버전 정보 조회' 가져오기 & version 변수에 저장. fetchone(): 쿼리 결과의 첫번째 행(row)을 튜플로 가져옴.

print(f"📊 {version[0][:60]}...") # 버전 정보 출력(60자만 출력)

cursor.close() # 커서 닫기
conn.close() # 연결 닫기

print("✅ 연결 테스트 완료!")