# 'load.py'는 리포트 데이터를 1. MySQL DB(daily_reports 테이블) 2. CSV 파일(reports/report_2024-11-19.csv) -> 이 2곳에 저장하는 파일입니다.
# ETL의 'L(Load)'를 담당합니다.

# 이로써 ETL 전(all) 단계를 수행했는데, ETL은 '추출(Extract)' -> '가공(Transform)' -> '저장(Load)'를 의미한다.

import pymysql
import logging
import os
from config import DB_CONFIG, PATHS
from contextlib import contextmanager

logger = logging.getLogger(__name__)

@contextmanager
def get_db_connection():
    """DB 연결 Context Manager (어제 배운 거!)"""

    # 연결 열기
    conn = pymysql.connect(
        **DB_CONFIG
    )

    try:
        yield conn
    
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

    finally:
        conn.close()

    # pass # 'pass'는 그냥 빈 공간으로 두고 싶을 때 쓰는 키워드이다. 그냥 두면 오류가 나니, pass로 오류가 안 나게 끔 한다.



def load_to_db(df,report_date):
    """
    DB에 리포트 저장 (UPSERT 방식)
    
    Parameters:
        df: transform에서 나온 DataFrame
        report_date: '2024-11-19' 형식
    """

    logger.info(f"DB 저장 시작: {report_date}")

    with get_db_connection() as conn:
        cursor = conn.cursor()

        # 기존 테이블 삭제
        cursor.execute("DROP TABLE IF EXISTS daily_reports")

        # 1. 테이블 없으면 생성
        create_table_sql = """
        create table if not exists daily_reports (
            report_date DATE,
            category VARCHAR(50),
            total_sales DECIMAL(15,2),
            total_quantity INT,
            avg_price DECIMAL(10,2),
            product_count INT,
            PRIMARY KEY (report_date, category) 
        )
        """

        cursor.execute(create_table_sql)

        # 2. UPSERT(INSERT ... ON DUPLICATE KEY UPDATE) 
        # UPSERT = UPDATE + INSERT. 

        """
        같은 날짜 데이터가 이미 DB에 있으면?
        → UPDATE (기존 데이터 수정)

        같은 날짜 데이터가 없으면?
        → INSERT (새로 추가)
        """


        """
        오늘 (11/21) 리포트 실행:
        - 11/21 데이터를 DB에 저장

        나중에 데이터 수정되어서 다시 실행:
        - 11/21 데이터를 또 INSERT하면?
        → 에러! (PRIMARY KEY 중복)

        해결책:
        - UPSERT 사용!
        → 있으면 UPDATE, 없으면 INSERT
        """
        
        insert_sql = """
        insert into daily_reports (report_date, category, total_sales, total_quantity, avg_price, product_count)
        values (%s, %s, %s, %s, %s, %s)
        on DUPLICATE KEY UPDATE # 중복 키(기본 키가 중복)가 있을 경우, 이런 식으로 업데이트를 하겠다는 의미.
            total_sales = values(total_sales), # 새 값을 넣겠다. 밑에도 동일한 구조. 기존의 값은 지워지고 나중의 값으로 채워진다(update).
            total_quantity = values(total_quantity),
            avg_price = values(avg_price),
            product_count = values(product_count)
        """

        # 3. Dataframe 각 행 처리
        for index, row in df.iterrows(): # 'df.iterrows()': dataframe의 각 행을 하나씩 순회(loop)한다는 의미.
            # index: 행 번호. row: 그 행의 데이터(딕셔너리처럼 쓸 수 있음)
            # 여기서는 index는 따로 안 쓰임. '_'로 변수명을 처리하여, 이 변수를 사용하지 않는다라고 표현해도 좋음.
            # df.iterrows()는 항상 'index,row' 2개를 돌려주므로, index를 사용 안 해도 꼭 써야하긴 한다.

            """
            for index, row in df.iterrows():
                print(f"Index: {index}")
                print(f"Row: {row}")
                print("---")
            ```

            **출력:**
            ```
            Index: 0
            Row: report_date       2024-11-19
                category          전자제품
                total_sales       150000
                total_quantity    50
                top_product       노트북
            ---
            Index: 1
            Row: report_date       2024-11-19
                category          의류
                total_sales       80000
                total_quantity    30
                top_product       청바지
            ---
            Index: 2
            Row: ...
            """

            # 깂 추출
            # date = row['report_date'] # 이미 파라미터로 report_date를 받고 있으므로, 제거해줌.
            category = row['category']
            sales = row['total_sales']
            quantity = row['total_quantity']
            avg_price = row['avg_price']
            prod_count = row['product_count']

            # SQL 실행
            cursor.execute(
                insert_sql,
                (report_date,category,sales,quantity,avg_price, prod_count)
            )

        # 4. commit(실제로 DB에 저장. 수정사항 반영.)
        conn.commit()
        logger.info(f"DB 저장 완료: {len(df)}건")


def load_to_csv(df, report_date):
    """
    CSV 파일로 저장
    
    저장 위치: reports/report_2024-11-19.csv
    """
    logger.info(f"CSV 저장 시작: {report_date}")

    # 1. reports 폴더 없으면 생성
    os.makedirs(PATHS['reports'], exist_ok=True) # 'exist_ok': 존재해도 괜찮다(에러 내지 말라.). 기본 값은 False인데, 만약 폴더가 이미 존재한다면, 에러를 발생시킨다. 하지만 True이면, 이미 있어도 그냥 넘어간다(에러가 안 남.).

    # 2. 파일명 생성
    filename = f"{PATHS['reports']}report_{report_date}.csv"

    # 3. 전체 경로 만들기
    filepath = os.path.join(PATHS['reports'], f"report_{report_date}.csv") # config.py의 PATHS에 만약에 'reports'에 './reports' 이렇게 '/' 없이 끝내면, 오류가 난다. 이를 전체 경로를 만듦으로써 자동으로 '/'를 붙여서 실수를 줄이게 끔 한다. 

    # 4. CSV 저장
    df.to_csv(filepath, index=False) # dataframe의 행 번호를 csv에 저장할지 말지를 결정.

    logger.info(f"CSV 저장 완료: {filename}")


def load_report(df, report_date):
    """
    메인 함수: DB와 CSV 둘 다 저장
    """

    try:
        # 1. DB 저장
        load_to_db(df, report_date)

        # 2. CSV 저장
        load_to_csv(df, report_date)

        logger.info("리포트 저장 완료!")

    except Exception as e:
        logger.error(f"저장 실패: {e}")
        raise


