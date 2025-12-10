import json
import boto3
import csv
from io import StringIO # IO: Input,Output(파일처럼 동작하는 메모리 기반 스트림 제공). # StringIO: 문자열을 파일처럼 다룰 수 있게 해주는 클래스. 
# lambda에서 StringIO를 쓰는 이유는, lambda는 /tmp(임시 폴더,512MB) 외에는 파일을 저장할 수 없다. 만약, df = pd.read_csv('local_file.csv') 이렇게 pandas로 읽어오면, lambda에 파일 저장 공간이 부족하다. 
# 근데 S3에서 데이터를 '문자열'로 가져와서 문자열을 '파일인 척' 만들고, pandas가 이를 '파일처럼' 읽으면 메모리에서 바로 처리할 수 있다.
# 즉, lambda는 저장 공간이 작고, 굳이 디스크에 저장할 필요없이 메모리에서 바로 처리하는 게 더 빠르고 효율적이다. 
# 디스크: HDD/SSD(영구 저장 공간, 느림), 메모리: RAM(CPU 옆에 있는 임시 저장 공간. 휘발성. 빠름(CPU 바로 옆이 있으므로)). 
from datetime import datetime

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
        csv_reader = csv.reader(StringIO(content)) # StringIO: 문자열을 파일처럼 다룰 수 있게 해주는 클래스.
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
        rows = list(csv_reader) # 리스트로 변환. # 좀 더 명확하게 말하자면, csv.reader()는 'iterator' 역할(한 번에 하나씩 한 행을 읽는 도구)을 하는데, list()를 하면 iterator를 모두 읽어서 list로 변환한다. 결국엔 위 예시 꼴로 나오는 것임.
        
        row_count = len(rows) - 1 # 헤더 제외.
        column_count = len(rows[0]) if rows else 0 # rows[0]은 헤더

        print(f"행 개수: {row_count}")
        print(f"열 개수: {column_count}")

        ## 4. 처리 결과를 processed/ 폴더에 저장.
        result = {
            'original_file' : key,
            'processed_time': datetime.now().isoformat(), # isoformat()은 datetime 객체를 문자열로 변환해주는 메서드.
            'row_count': row_count,
            'column_count': column_count,
            'status': 'success'
        }

        ## 5. 결과 파일명 생성
        original_filename = key.split('/')[-1] # samsung.csv(파일명만 추출) # 코드를 보고 충분히 유추할 수 있는데, '/로 구분하고, -1(맨 마지막) 것만 가져오니, 파일명만 가져오는 것이다.
        result_key = f"processed/{datetime.now().strftime('%Y/%m/%d')}/{original_filename}.json" # strftime: datetime 객체를 원하는 형식의 문자열로 변환.

        ## 6. 결과 저장
        s3.put_object(
            Bucket=bucket,
            Key=result_key,
            Body=json.dumps(result, ensure_ascii=False, indent=2) # ensure_ascii=False: 한글이 깨지지 않도록(한글이 그대로 나옴, True로 하면 한글이 유니코드로 변환됨.). # indent=2: 들여쓰기.
        )

        print(f"결과 저장: s3://{bucket}/{result_key}")

        return {
            'statusCode': 200, # 처리 상태를 의미. 200이면 정상.
            'body': json.dumps('File processed successfully!') # body 부분은 전달하고 싶은 메시지를 적는 곳이다. 근데 왜 'json.dumps()'로 감싸는 것일까? -> Lambda의 body는 반드시 문자열이어야 하는데, 딕셔너리나 리스트를 보내려면 JSON 문자열로 변환해야 함. 

            # """
            # # 단순 메시지
            # 'body': json.dumps('File processed successfully!')
            # # → '"File processed successfully!"'

            # # 복잡한 데이터
            # 'body': json.dumps({
            #     'message': 'Success',
            #     'row_count': 100,
            #     'file': 'data.csv'
            # })
            # # → '{"message": "Success", "row_count": 100, "file": "data.csv"}'
            # """
        }

    except Exception as e:
        print(f"에러 발생: {str(e)}")
        return {
            'statusCode': 500, # 500은 서버 내부 오류
            'body': json.dumps({'error':str(e)})
        }
        

        





