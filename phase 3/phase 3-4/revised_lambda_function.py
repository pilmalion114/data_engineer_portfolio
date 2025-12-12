## 기존 'phase 3-3'의 'lambda_function.py' 구조를 데이터베이스 연결로 바꿈. -> 기본 구조는 동일하나, 데이터베이스 부분만 추가된 거라고 이해하면 됨.
## 변경된 부분: 3번의 DictReader(), 4번 ~ 끝까지(데이터베이스 설정에 맞게 새롭게 구성함.)


import json
import boto3
import csv
from io import StringIO # IO: Input,Output(파일처럼 동작하는 메모리 기반 스트림 제공). # StringIO: 문자열을 파일처럼 다룰 수 있게 해주는 클래스. 
# lambda에서 StringIO를 쓰는 이유는, lambda는 /tmp(임시 폴더,512MB) 외에는 파일을 저장할 수 없다. 만약, df = pd.read_csv('local_file.csv') 이렇게 pandas로 읽어오면, lambda에 파일 저장 공간이 부족하다. 
# 근데 S3에서 데이터를 '문자열'로 가져와서 문자열을 '파일인 척' 만들고, pandas가 이를 '파일처럼' 읽으면 메모리에서 바로 처리할 수 있다.
# 즉, lambda는 저장 공간이 작고, 굳이 디스크에 저장할 필요없이 메모리에서 바로 처리하는 게 더 빠르고 효율적이다. 
# 디스크: HDD/SSD(영구 저장 공간, 느림), 메모리: RAM(CPU 옆에 있는 임시 저장 공간. 휘발성. 빠름(CPU 바로 옆이 있으므로)). 
from datetime import datetime
import os # 환경 변수 접근용
import pymysql # MySQL 연결용



# 1. S3 클라이언트 생성
s3 = boto3.client('s3')

def lambda_handler(event,context):
    ## 1. S3 이벤트에서 버킷, 파일 정보 추출
    bucket = event['Records'][0]['s3']['bucket']['name']
#     """
#     ※event 구조 - 중첩된 딕셔너리 구조.

#     event = {
#     'Records': [                    # 리스트 (여러 이벤트 가능)
#         {
#             's3': {                 # S3 관련 정보
#                 'bucket': {         # 버킷 정보
#                     'name': 'my-bucket'  # ← 여기!
#                 },
#                 'object': {         # 파일 정보
#                     'key': 'data/file.csv'
#                 }
#             }
#         }
#     ]
# }
#     """

    key = event['Records'][0]['s3']['object']['key']

    print(f"처리 시작: s3://{bucket}/{key}")

    try:
        ## 2. S3에서 파일 읽기
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8') # 충분히 유추 가능함.

        ## 3. CSV 분석
        csv_reader = csv.DictReader(StringIO(content)) # StringIO: 문자열을 파일처럼 다룰 수 있게 해주는 클래스. # 개정된 버전에서는 DictReader로 변경됨.
        """
        csv.reader가 읽으면 이렇게 반환해준다.

                rows = [
            ['name', 'age', 'city'],      # 첫 번째 행 (헤더)
            ['Alice', '25', 'Seoul'],     # 두 번째 행
            ['Bob', '30', 'Busan'],       # 세 번째 행
            ['Charlie', '35', 'Daegu']    # 네 번째 행
        ]

        리스트 안에 리스트 꼴(2차원 리스트)
        """

        # DictReader: 첫 번째 행을 헤더로 인식하고, 각 행을 딕셔너리로 반환
        # 예: {'date': '2024-01-01', 'open': '85000', ...}


        rows = list(csv_reader) # 리스트로 변환. # 좀 더 명확하게 말하자면, csv.reader()는 'iterator' 역할(한 번에 하나씩 한 행을 읽는 도구)을 하는데, list()를 하면 iterator를 모두 읽어서 list로 변환한다. 결국엔 위 예시 꼴로 나오는 것임.
        
        #row_count = len(rows) - 1 # 헤더 제외.
        row_count = len(rows)
        column_count = len(rows[0]) if rows else 0 # rows[0]은 헤더

        print(f"행 개수: {row_count}")
        print(f"열 개수: {column_count}")

        ## 4. 처리 결과를 processed/ 폴더에 저장.
        # result = {
        #     'original_file' : key,
        #     'processed_time': datetime.now().isoformat(), # isoformat()은 datetime 객체를 문자열로 변환해주는 메서드.
        #     'row_count': row_count,
        #     'column_count': column_count,
        #     'status': 'success'
        # }

        ## (new)4. RDS 연결
        ## 환경 변수에서 DB 접속 정보 가져오기
        ## os.environ['KEY']: Lambda 환경 변수에서 값 읽기
        connection = pymysql.connect(
            host=os.environ['DB_HOST'], # RDS 엔드포인트
            port=int(os.environ['DB_PORT']), # 3306 (문자열을 정수로 변환)
            user=os.environ['DB_USER'], # admin
            password=os.environ['DB_PASSWORD'], # RDS 비밀번호
            database=os.environ['DB_NAME'], # samsung_stock
            charset='utf8mb4', # 한글 지원 문자셋
            cursorclass=pymysql.cursors.DictCursor # DictCursor: 딕셔너리 형태로 결과를 반환.
        )

        print("RDS 연결 성공!")

        ## 5. 결과 파일명 생성
        # original_filename = key.split('/')[-1] # samsung.csv(파일명만 추출) # 코드를 보고 충분히 유추할 수 있는데, '/로 구분하고, -1(맨 마지막) 것만 가져오니, 파일명만 가져오는 것이다.
        # result_key = f"processed/{datetime.now().strftime('%Y/%m/%d')}/{original_filename}.json" # strftime: datetime 객체를 원하는 형식의 문자열로 변환.

        ## (new)5. 커서 생성
        cursor = connection.cursor()


        ##############################################################################################################################################################################
        ## 여기서부터 새로운 코드만 나옴.(기존 코드 가독성을 위해서 다 지워버림.)
        ##############################################################################################################################################################################

        ## 6. 주식 데이터 저장 (samsung_daily 테이블)
        insert_count = 0

        for row in rows:
            ## BOM 제거: 첫 번째 키 정규화
            ## row의 첫 번째 키에 BOM이 있을 수 있음.(인코딩 문제) -> 즉, 첫 번째 열(Date)에 BOM이 붙어 있는 것임. # BOM 문제는 저번에도 인코딩 문제에서 나왔는데, utf-8로 설정되어 그럼. utf-8-sig로 바꾸면 됨.
            date_key = list(row.keys())[0] # 실제 첫 번째 컬럼명 가져오기.

            sql = """ 
            INSERT INTO samsung_daily (date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s)
            on Duplicate key update -- upsert 방식.
                open = values(open),
                high = values(high),
                low = values(low),
                close = values(close),
                volume = values(volume)
            """

            cursor.execute(sql, (row[date_key], row['Open'], row['High'], row['Low'], row['Close'], row['Volume'])) # csv 파일 대/소문자 구분 필수... (이거 때문에 로그 애 많이 먹음...ㅠ). # BOM 있어도 작동! # 기존 csv에서의 'Change'열은 무시함.
            insert_count += 1
            
        print(f"samsung_daily 테이블에 {insert_count}건 처리 완료")

        ## 7. 처리 이력 저장 (processing_log 테이블)
        log_sql = """
        INSERT INTO processing_log 
        (original_file, processed_time, row_count, column_count, status)
        VALUES (%s, %s, %s, %s, %s)
        """

        cursor.execute(log_sql,(
            key, # 원본 파일명
            datetime.now(), # 처리 시간(현재 시간)
            row_count, # 행 개수
            column_count, # 열 개수
            'success' # 처리 상태
        ))

        print("processing_log 테이블에 이력 저장 완료")

        ## 8. commit
        connection.commit()
        # commit(): 지금까지의 INSERT/UPDATE를 DB에 최종 반영
        # commit() 전까지는 임시 상태 (rollback 가능)
        # commit() 후에는 영구 저장 (되돌릴 수 없음)

        print("DB 커밋 완료!")

        ## 9. 연결 종료
        cursor.close() # 커서 닫기
        connection.close() # 연결 닫기
        # 리소스 정리: 사용 후 반드시 닫아야 함 (메모리 누수 방지)

        print(f"처리 완료: {row_count}건의 데이터 저장")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data saved to RDS successfully!',
                'rows_processed': row_count
            })
        }

    except Exception as e:  # ← 이 줄 추가!
        print(f"에러 발생: {str(e)}")

        # 에러 발생 시 연결 종료 (열려있다면)
        try:
            if 'connection' in locals() and connection.open:
                connection.close()
        except:
            pass # 그냥 넘어가기.
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

        





