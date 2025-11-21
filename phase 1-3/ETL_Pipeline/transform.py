# 'transform.py'는 추출한 원본 데이터를 리포트 형식으로 가공하는 파일입니다.
# ETL의 'T'(Transfrom)을 담당합니다.

import pandas as pd
import logging

## 로그 설정
logger = logging.getLogger(__name__)

## 데이터 검증 함수(선택)
def validate_data(df):
    """
    데이터 유효성 검사
    
    Args:
        df: 원본 데이터
        
    Returns:
        bool: 유효하면 True
    """

    # 필수 칼럼 체크
    required_cols = ['date','category','product','quantity','price']

    if not all(col in df.columns for col in required_cols): # 실제 df의 column들과 required_cols의 col에서 전부 일치하지 않을 때. 즉, 하나라도 틀린 경우가 나올 때 
        logger.error("필수 칼럼 누락!")
        return False # bool 형이므로

    # 이상치 체크 (음수 값)
    if (df['quantity']<0).any():
        logger.warning("음수 수량 발견!")

    if (df['price']<0).any():
        logger.warning('음수 가격 발견!')

    return True 


## 메인 transform 함수
def transform_daily_report(df):
    """
    원본 판매 데이터를 일일 리포트 형식으로 변환
    
    Args:
        df (DataFrame): 원본 데이터
            - 컬럼: date, category, product, quantity, price
    
    Returns:
        DataFrame: 리포트 데이터
            - 컬럼: category, total_sales, total_quantity, avg_price, product_count
    """

    try:
        logger.info("데이터 변환 시작!")

        # 1. 데이터 검증:
        if not validate_data(df): # validate_data(df)의 함수 리턴값이 true가 아니면, None(Null임.)을 반환
            return None
        
        # 2. 총 매출 계산 (quantity * price)
        df['total_sales'] = df['quantity'] * df['price']

        # 3. 카테고리별 집계
        """
        category_total_price = df.groupby('category')['total_sales'].sum()
        category_total_quantity = df.groupby('category')['quantity'].sum()
        category_avg_price = df.groupby('category')['price'].mean()

        """

        # agg() 함수를 통해 한번에 집계하기

        report_df = df.groupby('category').agg({
            'total_sales': 'sum', # 총 매출
            'quantity': 'sum', # 총 수량
            'price': 'mean', # 평균 가격
            'product': 'count' # 상품 갯수
        })
        
        # 4. 칼럼명 정리(선택)
        report_df.columns = ['total_sales', 'total_quantity', 'avg_price', 'product_count']
        

        # 5. 인덱스(category)(행)를 칼럼으로
        # 4번까지의 코드 결과를 보면, 'groupby'에 의해서 category가 행으로 감. 이를 열로 바꿔주고, 인덱스에는 0,1,2,...가 들어감.
        report_df = report_df.reset_index()


        logger.info(f"변환 완료: {len(report_df)}개 카테고리")
        return report_df


    except Exception as e:
        logger.error(f"데이터 변환 실패: {e}")
        return None
    

## 테스트 코드
if __name__ == "__main__":
    import logging
    from config import LOG_CONFIG

    logging.basicConfig(**LOG_CONFIG)

    # extract.py에서 데이터 가져오기
    from extract import extract_sales_data

    print("=== transform.py 테스트 ===")

    # 1. 데이터 추출
    df = extract_sales_data('2025-11-17')
    print(type(df)) # Dataframe 자료형으로 나옴.

    if df is not None:
        print(f"\n[추출된 원본 데이터: {len(df)}건]")
        print(df.head())

        # 2. 데이터 변환

        report = transform_daily_report(df)

        if report is not None:
            print(f"\n[변환된 리포트: {len(report)}개 카테고리]")
            print(report)

        else:
            print("\n❌ 변환 실패!")

    else:
        print("❌ 데이터 추출 실패!")