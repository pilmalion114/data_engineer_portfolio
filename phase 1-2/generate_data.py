#(중요!) 버전 충돌로 python venv 가상환경 만들어서 독립적인 환경에서 확실하게 진행함.

## 1. 라이브러리 설치 및 DB 연결

# import mysql.connector # from 부분 한 줄에 쓰면 오류남. 라이브러리 오류로 인해 밑에 걸로 바꿈.
import pymysql # 이게 더 안정적이라고 함.
from faker import Faker
import random 
from datetime import datetime, timedelta

# faker 인스턴스(객체) 생성
fake = Faker('ko_KR') # 한국어 데이터

print("라이브러리 로드 완료!")
print("MySQL 연결 테스트 시작...")

try:

# MySQL 연결. 로컬 환경 + 나처럼 xampp 사용하고 있으면, 해당 정보를 'C:\xampp\phpMyAdmin\config.inc.php' 여기서 확인하면 됩니다.
# pymysql로 변경.
    conn = pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='1234',
        database='shop_db'
    )

    print("✅ MySQL 연결 성공!")

    #DB 목록 확인
    cursor =  conn.cursor() # Cursor = DB와 대화하는 포인터(손가락). conn은 DB와의 연결 자체를 의미하고, cursor는 실제 작업 수행(SQL 실행)을 한다.
    cursor.execute("show databases")
    databases = cursor.fetchall() # fetch는 가져오다의 의미.

    print("\n📂 사용 가능한 데이터베이스:")
    for db in databases:
        print(f" - {db[0]}") # f는 f-string, f는 format의 약자로, '문자열 안에 변수를 삽입할 수 있게 해주는 포멧'입니다. {}에는 변수,연산,함수 호출,인덱싱 등 어떠한 것도 가능하다고 합니다.
        # db[0]은 db의 튜플 안에 있는 0번째 인덱스를 의미하는데, 이 0번째 행은 'db 이름'을 의미합니다.

    #cursor.close()
    #conn.close()

except Exception as e:
    print(f"❌ 연결 실패: {e}")

## 2. user_logs 테이블 만들기

# 기존에 해당 테이블이 있으면 먼저 삭제 후 진행하기

print("\n🗑️  기존 테이블 확인 중...")
cursor.execute("drop table if exists user_logs") # 이미 여기서 drop table을 해서 ## 3번에 truncate해도 어차피 빈 테이블로 항상 결과가 나옴.
print("✅ 기존 테이블 삭제 완료!")

# 새 테이블 생성(user_logs)
print("\n📋 user_logs 테이블 생성 중...")

cursor.execute("""
    create table user_logs (
               log_id int primary key auto_increment,
               user_id int not null,
               action varchar(50),
               created_at datetime
               ) Engine = InnoDB
       """        )

# 위에서 " 3개(삼중 따옴표)는 여러 줄 문자열을 위해서 사용한 것이고, innodb 엔진은 '트랜잭션 지원,fk 지원,복구 기능 지원'으로 실무 표준 엔진이고, commit()의 의미는 수정사항을 DB에 실제 반영하라는 의미이다.(마치 github commit 같이 생각하면 됨.)

conn.commit()
print("✅ 테이블 생성 완료!")

# 테이블 구조 확인
print("\n📊 테이블 구조:")

cursor.execute("Describe user_logs")
columns = cursor.fetchall()
for col in columns:
    print(f" = {col[0]}: {col[1]}") # 실제 column을 가져오는 코드 부분. 테이블에서 보면 col[0]은 칼럼명을, col[1]은 데이터 타입을 의미함.

#cursor.close()
#conn.close()

print("\n" + "=" * 50)
print("🎉 테이블 생성 완료!")
print("=" * 50)


## 3. user_logs 테이블 안에 더미데이터 100만개 만들기

## cf.) 우리가 전에 데이터베이스(shop_db)에 'users','products','orders' 3개의 테이블을 만들었으나, 이는 'erd와 실제 테이블 설계'를 설명하기 위함이고,
## 지금은 user_logs 테이블을 따로 만들어서 여기에 더미데이터를 100만개 만들어서 index 유무에 따른 성능 비교 차이를 수행할 것을 미리 알립니다.(혼동 방지용) -> 계획이 기존에서 변경됨을 알림.
## 이는 실무에서도 로그 기록을 활용하여 성능 테스트함을 모방하여 진행함을 알림.

# 기존 데이터 삭제
print("\n🗑️  기존 데이터 확인 중...")
cursor.execute("select count(*) from user_logs")
existing_count = cursor.fetchone()[0] # [0]은 튜플에서 0번째 인덱스인 'count'를 의미함.

if existing_count > 0:
    print(f"⚠️  기존 데이터 {existing_count:,}건 발견!")
    cursor.execute("truncate table user_logs") # truncate(자르다,잘라내다.): 테이블 구조는 유지하고 데이터만 전부 삭제(빠름). 'delete from user_logs'도 가능하지만, truncate가 더 빠르고 효율적이다.
    conn.commit()
    print("✅ 기존 데이터 삭제 완료!")
else:
    print("✅ 테이블이 비어있습니다.")

    
print("\n" + "=" * 50)
print("📦 더미데이터 생성 시작!")
print("=" * 50)

# 생성할 데이터 수
Total_Rows = 1000000 # 100만개의 더미데이터 생성

Batch_Size = 1000 # 한번에 1000건씩 insert. 
actions = ['login','logout','view','click', 'purchase', 'search', 'download', 'upload']

print(f"📊 목표: {Total_Rows:,}건")
print(f"📦 배치 크기: {Batch_Size}건\n")

import time
start_time = time.time() # 시작을 알림.

for batch_num in range(0,Total_Rows,Batch_Size):
    # 배치 데이터 생성
    batch_data = []

    for i in range(Batch_Size):
        if batch_num + i >= Total_Rows:
            break

        user_id = random.randint(1,10000) # 사용자 1~10000
        action = random.choice(actions)
        created_at = fake.date_time_between(
            start_date='-1y', # 1년 전부터
            end_date='now'
        )

        batch_data.append((user_id,action,created_at))


    # 배치 insert
    cursor.executemany(
        "Insert into user_logs (user_id, action, created_at) Values (%s, %s, %s)", # %s는 'placeholder' -> 여기에 값을 넣어달라는 자리 표시이다.
        batch_data
    )
    conn.commit()

    # 진행상황 표시
    progress = min(batch_num + Batch_Size, Total_Rows) # 두 값 중 최솟값을 선택하여 진행상황을 표현함.
    percentage = (progress/Total_Rows) * 100 # 진행상황을 % 형식으로 변형하여, 진행률로 표현함.
    print(f" 진행: {progress:,} / {Total_Rows:,} ({percentage:.1f}%)", end='\r') # ':,' -> 천 단위 콤마라고 함. ':.1f'는 소수점 1자리까지. end='\r'은 같은 줄에 덮어쓰기를 의미한다.


elapsed_time = time.time() - start_time # 총 걸린 시간 기록

print("\n\n✅ 더미데이터 생성 완료!")
print(f"⏱️  소요 시간: {elapsed_time:.2f}초")

#최종 확인
cursor.execute("select count(*) from user_logs")
count = cursor.fetchone()[0] # 마찬가지로 튜플의 0번째 인덱스만을 가져오는 것인데, 이는 count의 정보를 나타냄.
print(f"📊 생성된 데이터: {count:,}건")

cursor.close()
conn.close()

print("\n" + "=" * 50)
print("🎉 모든 작업 완료!")
print("=" * 50)


### 4. test_index.py에서 이어서 진행...