# 이 역시 'airflow'에 맞게끔 리팩토링하는 작업이다.

# 삼성전자 주식 데이터를 웹 MySQL에 적재하는 모듈

import pymysql
import pandas as pd

# 1. DB 설정
DB_CONFIG = {
    'host': 'host.docker.internal', # Docker가 로컬 PC를 가리키는 주소.

    # 로컬에서 MySQL 접속할 때:
    # - 127.0.0.1 = 내 컴퓨터

    # Docker 컨테이너 안에서:
    # - 127.0.0.1 = 컨테이너 자기 자신 (MySQL 없음!)
    # - host.docker.internal = 내 컴퓨터 (MySQL 있음!)

    #'''
    # <요약> 그니깐 '127.0.0.1'은 Docker Container 관점에서 봤을 때, 나 자기자신 컨테이너이기 때문에 이 방 안에는 MySQL이 없으므로, 'host.docker.internal'이라는 내가 공유하는 집(컴퓨터)에서 MySQL을 찾아서 자동으로 연결한다는 의미.
    #'''

    'user': 'root',
    'password': '1234',
    'database': 'samsung_stock',
    'charset': 'utf8mb4' # 인코딩 방식
}

# 2. 테이블 생성 SQL
# 밑에 volume에서 'int'말고 범위를 더 크게 하고 싶으면 'Bigint'로 하면 된다. -> 지금은 거래량이 몇천만개 수준이라 괜찮음(int가 21억개 정도 보유할 수 있음)
# 'change'는 예약어라 백틱(``)으로 감쌈
CREATE_TABLE_SQL = """
create table if not exists samsung_daily (
    date DATE Primary Key,
    open int,
    high int,
    low int,
    close int,
    volume int, 
    `change` float 
)
"""

# 3. 테이블 생성 함수
def create_table():

    # DB 연결
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # SQL 실행
    cursor.execute(CREATE_TABLE_SQL)

    # 반영 및 종료
    conn.commit()
    cursor.close()
    conn.close()

    print("테이블 생성 완료!")

# 4. 데이터 적재 함수
def load_data(input_file): # 파라미터 추가.
    """
    [설계 결정] 파생변수 저장 방식

    Q. 파생변수(MA_5, MA_20, Volatility 등)를 어떻게 처리할 것인가?
    - 방식 1) 미리 계산해서 DB에 저장 → 조회 빠름, 저장 공간 사용
    - 방식 2) 원본만 저장, 쿼리 시점에 계산 → 저장 공간 절약, 조회 시 계산 필요

    선택: 방식 2 (쿼리 시점 계산)
    이유: Window Function 실전 연습 기회 + SQL 실력 향상

    ※ 실무 참고사항
    - 대용량 데이터에서는 오히려 미리 계산해서 저장하는 게 효율적
    - 자주 조회하는 지표일수록 미리 저장하는 경우가 많음
    - 면접 답변: "학습 목적으로 쿼리 시점 계산을 선택했지만, 
    실무에서 대용량이라면 자주 쓰는 지표는 미리 계산해서 저장하는 게 효율적이라는 것도 알고 있습니다."

    """

    # 1. csv 파일 읽기(삼성주식 원본 데이터) -> 쿼리 시점에서 계산하기로 하였으므로, 원본 데이터만 DB에 저장한다.
    df = pd.read_csv(input_file) 

    # 2. DB 연결
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # 3. 데이터 insert(upsert는 굳이 필요 x -> 이미 끝난 과거 데이터 자료이니깐, 수정될 일이 없다고 판단함.) -> 하지만 Claude의 반문이, 코드를 두번 실행하면 이미 있던 데이터가 중복되므로 upsert로 해결해야한다고 말함.(insert로 그냥 하면 에러가 난다고 함.)
    insert_sql = """
    insert into samsung_daily (date, open, high, low, close, volume, `change`)
    values (%s, %s, %s, %s, %s, %s, %s)
    on DUPLICATE KEY UPDATE
        open = values(open),
        high = values(high),
        low = values(low),
        close = values(close),
        volume = values(volume),
        `change` = values(`change`)
    """
    
    for index, row in df.iterrows():

        # 값 추출
        # csv를 df에서 불렀는데, 어쨋든 csv에 저장된 열 이름대로 대소문자 구분해서 작성해야한다.
        date = row['Date']
        open = row['Open']
        high = row['High']
        low = row['Low']
        close = row['Close']
        volume = row['Volume']
        #change = row['`change`'] # 백틱은 SQL에서만 예약어 구분용이다. Dataframe 칼럼명에서는 필요가 없다.
        change = row['Change']

        cursor.execute(insert_sql, (date,open,high,low,close,volume,change)) # Upsert로 했으므로, df(dataframe)에서 각 열에 해당하는 각 행 값들을 가져와서 for문을 통해 직접 값들을 넣어줘야한다.('phase 1-3'의 'load.py' 참고)

    # 4. 종료
    conn.commit()
    cursor.close()
    conn.close()

    print(f"데이터 {len(df)}건 적재 완료!")


# 5. 쿼리 시점 계산(파생변수 계산)(Window Function)
# 원본 데이터만 저장했으므로, 파생변수는 SELECT할 때 계산한다.
# - Phase 2-3에서 만들었던 파생변수들:
#   - MA_5: 5일 이동평균
#   - MA_20: 20일 이동평균
#   - Volatility: 당일 변동성 (High - Low)
#   - Volume_MA_5: 5일 거래량 이동평균
#   - Price_Range: 당일 가격 범위 비율 ((High - Low) / Open * 100)

def get_data_with_query():
    """파생변수가 포함된 데이터 조회 (쿼리 시점 계산)"""

    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    query = """
    select date, open, high, low, close, volume, `change`,

        -- 5일 이동평균
        avg(close) over(order by date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as MA_5,

        -- 20일 이동평균
        avg(close) over(order by date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as MA_20,

        -- 당일 변동성(Window Function 필요 없음)
        high - low as Volatility, -- 기본적으로 sql문은 select한 열에 있는 행들을 다 도니깐(where절로 조건절 세우지 않는 이상)(자동 for문 같은 느낌), 이렇게 단순히 열(속성)의 차로 표현해도 된다. Python에서는 일일이 for문을 통해 df(dataframe)에 있는 값들을 불러냈다면, sql에서는 이를 자동으로 해주는 느낌으로 이해하면 된다.

        -- 5일 거래량 이동평균
        avg(volume) over(order by date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as Volume_MA_5,

        -- 당일 가격 범위 비율(Window Function 필요 없음)
        (high - low) / open * 100 as Price_Range

    from samsung_daily
    order by date

    """

    cursor.execute(query)
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    return results






if __name__ == "__main__":
    create_table()
    load_data("/opt/airflow/data/samsung_2024-11-28_2025-11-27.csv") # 파라미터 인자 전달. Docker 경로로 수정.

    # 파생변수 포함 데이터 조회
    results = get_data_with_query() # get_data_with_query()에서 나온 return 값 'results'랑은 다른 main문에 설정한 새로운 지역변수 results
    
    # '''for row in results[:5]: # 앞에 5개만 출력
    #    print(row)'''  # 별로 안 이쁘게 나와서 밑에 이쁘게 나오도록 df 형식으로 출력함.
    # cf.)놀라운 사실: '''(여러 줄 문자열)은 사실상 주석이 아니라고 함.(python 공식 주석이 아니라고 함.) #만이 진짜 주석이고, ''' 이거는 그냥 문자열인데 변수에 안 담기면 사라진다고 함. 그래서 주석처럼 동작하는 것임. 그래서 써도 에러가 안 나고 돌아가는 것임.
    
    # 결과를 DataFrame으로 변환
    df_result = pd.DataFrame(
        results,
        columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'change',
                   'MA_5', 'MA_20', 'Volatility', 'Volume_MA_5', 'Price_Range']
    )

    print(df_result.head())

    # 위 5개 파생변수 결과들만 CSV로 저장(포트폴리오용) 
    # 파생변수를 쿼리 시점 계산으로 한 거라 CSV로 저장하는 것은 논리상 어긋나나, 포트폴리오용으로 간단하게 보여주고 싶어서 이것만 CSV로 저장함.
    import os
    # from datetime import datetime # 딱히 여기서는 쓸 곳은 없지만 그냥 습관상 ㅎㅎ...

    os.makedirs("/opt/airflow/data", exist_ok=True) # 이 부분 Docker 경로로 바꾸기

    df_result.head().to_csv("/opt/airflow/data/sample_with_derived.csv", index=True) # index=True가 기본값이다.
    # sql에서는 내가 date를 pk로 해서 자동 인덱스가 설정됐지만, 이 df에서는 그냥 date는 일반적인 열(속성)이기 때문에 default index인 숫자(auto_increment 같은)가 나오는 것임.


    
