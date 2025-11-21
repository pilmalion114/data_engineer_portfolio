# 이 파일에는, 'configuration':' 설정' 이라는 뜻에 맞게, 설정에 관한 코드를 작성하는 곳입니다.


## 1. DB 연결 정보
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '1234',
    'database': 'test_pipeline'
}


## 2. 로그 설정
LOG_CONFIG = {
    'filename': './logs/daily_report.log',
    'filemode': 'a', # 보통은 append를 쓴다고 함.
    'format': '%(asctime)s - %(levelname)s - %(message)s',
    'level': 'INFO',
}


## 3. 파일 경로
PATHS = {
    'reports': './reports/', # CSV 저장할 폴더
    'logs': './logs'
}


