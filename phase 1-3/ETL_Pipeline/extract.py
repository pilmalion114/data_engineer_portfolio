# 'extract.py'는 데이터베이스에서 원하는 데이터를 추출하는 모듈입니다.
# 'ETL 파이프라인'에서 'E(Extract)' 부분입니다.

import pymysql
import pandas as pd
import logging
from config import DB_CONFIG, LOG_CONFIG
from contextlib import contextmanager

## 로그 설정
logger = logging.getLogger(__name__) # '__name__':파이썬의 특수 변수. 현재 파일(모듈)의 이름을 담고 있는 변수. 비슷한 개념으로는, '__main__'(실행 파일)이 있다.
# 어느 파일에서 발생한 로그인지 알 수 있음. '__name__' 안 쓰면, 기본인 'root'에서 가져옴.
# 뒤 코드에서 보면, 'logger.info','logger.error' 이렇게 썼는데, 이를 통해 자동으로 'extract.py'에서 일어난 로그로 기록한다.

## context Manager
@contextmanager
def get_db_connection():

    # 연결 열기
    conn = pymysql.connect(
        **DB_CONFIG # 'config.py'에 미리 적어 둠. '**'의 의미는 '딕셔너리 언패킹'이라는 의미이다.
    )

    try:
        yield conn # yield에서 잠시 멈추고, conn을 with에 넘김.

    except Exception as e:
        logger.error(f"Error: {e}") # logging이 아닌 logger
        raise

    finally:
        conn.close()


## 메인 함수 
def extract_sales_data(target_date):
    """
    특정 날짜의 판매 데이터를 DB에서 추출
    
    Args:
        target_date (str): 추출할 날짜 (예: '2024-11-19')
    
    Returns:
        DataFrame: 판매 데이터 or None (실패 시)
    """
     
    try:
        with get_db_connection() as conn: # conn을 받아서 실제 작업을 하는 곳
             query = f"""
                    select date, category, product, quantity, price
                    from sales
                    where date = '{target_date}'
            """
             
             df = pd.read_sql(query,conn)

             logger.info(f"{target_date} 데이터 {len(df)}건 추출 완료!")
             return df
        
    except Exception as e:
        logger.error(f"데이터 추출 실패:{e}")
        return None


## 테스트 코드(extract.py는 모듈이지 실행 파일이 아니므로 따로 테스트 코드 작성함.)
if __name__ == "__main__":
    # 로깅 기본 설정
    import logging
    logging.basicConfig(**LOG_CONFIG)

    # 테스트 실행
    print("=== extract.py 테스트 시작 ===")

    # DB에 어떤 날짜가 있는지 확인.
    print("\n[1. DB에 있는 날짜들 확인]")

    try:
        with get_db_connection() as conn:
            df_dates = pd.read_sql("select distinct date from sales order by date asc", conn)
            print(df_dates)

            if len(df_dates) > 0:
                # 첫번째 날짜로 테스트
                test_date = df_dates.iloc[0]['date'] # 'iloc'은 pandas에서 데이터프레임의 정수 위치를 기반으로 데이터를 선택하는 인덱싱 방법. 여기서는 [0]은 0번째 행을 의미함.
                                                     # 'loc' vs 'iloc': loc는 '라벨(label)(행/열 이름) 기반 인덱싱', iloc은 '정수 기반 인덱싱'이다.
                print(f"\n[2. {test_date} 날짜로 테스트]")

                result = extract_sales_data(str(test_date))

                if result is not None:
                    print(f"\n✅ 성공! {len(result)}건 추출됨")
                    print("\n[처음 5줄]")
                    print(result.head())

                else:
                    print("\n❌ 추출 실패!")

            else:
                print("⚠️ sales 테이블에 데이터가 없어요!")

    except Exception as e:
        print(f"❌ 에러 발생: {e}")