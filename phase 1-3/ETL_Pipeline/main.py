# 'main.py'는 모든 모듈들을 연결하여 전체 ETL 파이프라인을 실행하는 파일이다.
# 'main.py' 실행 -> Extract -> Transform -> Load -> 완료

import logging
from datetime import datetime,timedelta
from extract import extract_sales_data
from transform import transform_daily_report
from load import load_report
from config import LOG_CONFIG

# 1. 전역 로깅 설정
logging.basicConfig(**LOG_CONFIG)
logger = logging.getLogger(__name__)

def main():
    """
    메인 ETL 파이프라인
    
    처리 순서:
    1. Extract: DB에서 데이터 추출
    2. Transform: 데이터 가공
    3. Load: 결과 저장 (DB + CSV)
    """

    try:
        # 실행 시작 로그
        logger.info("=" * 50)
        logger.info("ETL 파이프라인 시작")
        logger.info("=" * 50)

        # 날짜 설정
        today = datetime.now() # 오늘('2025-11-21')
        ten_days_ago = today - timedelta(days=10) # 10일 전('2025-11-11')
        report_date = ten_days_ago.strftime('%Y-%m-%d') # 문자열로 변환

        # 1. Extract
        logger.info("Step 1: 데이터 추출 시작")
        df = extract_sales_data(report_date)
        logger.info(f"추출 완료: {len(df)}건")

        # 2. Transform
        logger.info("Step 2: 데이터 변환 시작")
        report_df = transform_daily_report(df)
        logger.info(f"변환 완료: {len(report_df)}건")

        # 3. Load
        logger.info("Step 3: 데이터 저장 시작")
        load_report(report_df, report_date)

        # 완료 로그
        logger.info("=" * 50)
        logger.info("ETL 파이프라인 완료!")
        logger.info("=" * 50)


    except Exception as e:
        logger.error("=" * 50)
        logger.error(f"파이프라인 실패: {e}")
        logger.error("=" * 50)
        raise

if __name__ == "__main__":
    main()
