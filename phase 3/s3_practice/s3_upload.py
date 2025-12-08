# S3에 파일 업로드하는 스크립트

import boto3
import csv
import os

# 1. CSV 파일에서 AWS 자격증명 읽기(보안상 노출이 되면 절대 안 됨!)
csv_file = 'boto3-user_accessKeys.csv'

with open(csv_file,'r', encoding='utf-8-sig') as f: # utf-8-sig로 열면 BOM 자동 제거!
    reader = csv.DictReader(f) # 'DictReader'는 'Dictionary'+'Reader'로, CSV 파일을 읽되, 각 행을 '딕셔너리' 형태로 반환함. 키(컬럼명)로 접근함. # cf.) 'csv.reader'는 각 행을 '리스트'로 반환(인덱스(숫자)로 접근).
    credentials = next(reader) # credentials: 자격 증명. # next 문법은 csv에서 한 행 전체를 가져온다. # csv.DictReader는 첫번째 행을 자동으로 헤더로 인식하고, next(reader)를 통해서 그 다음 한 행 전체를 가져와서, 키-값 구조로 만듦. # csv.reader는 첫번째 행,두번째 행 둘다 리스트로 반환함.

    ## (테스트) 실제 헤더 이름 확인!
    print("=" * 50)
    print("CSV 파일의 헤더(키) 목록:")
    print("=" * 50)
    for key in credentials.keys():
        print(f"  - '{key}'") # 인코딩 방식이 'utf-8-bom'으로 되어있어서 이상하게 나왔음. 위의 with open 코드 참고하기()
    print("=" * 50)

AWS_ACCESS_KEY_ID = credentials['Access key ID']
AWS_SECRET_ACCESS_KEY = credentials['Secret access key']
AWS_REGION = 'ap-northeast-2' # 서울

# 2. 클라이언트 생성
s3 = boto3.client(
    's3',
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# 3. 버킷 이름
BUCKET_NAME = 'samsung-stock-data-pilmalion114'

# 4. 로컬 파일 경로
## phase 2에서 만든 CSV 파일 경로로 수정!(phase 2-5는 Docker가 관리하므로 Permission Denied라는 에러가 떠서 phase 2-2에서 가져옴.)
local_file = r'C:\Users\dc\Desktop\새로운 포트폴리오를 위한 폴더\데이터,AI\포트폴리오용\데이터 엔지니어링\실습\data_engineer_portfolio\phase 2\phase 2-2\Data\samsung_2024-11-28_2025-11-27.csv' # 'r'은 윈도우 경로를 넣을 때, '\'을 '이스케이프 문자(문자열 내에서 특수한 의미를 가진 문자를 표현하게 도와주는 문자. ex. \n,\t 등)'로 해석하지 '않게' 도와준다. 즉, 문자 그대로 역슬래쉬('\') 취급한다.

# 5. S3에 저장될 경로
s3_file = 'raw/samsung_from_python.csv' # S3의 raw 폴더에 새로운 csv 파일이 생성된다.

# 6. 업로드 실행
print("=" * 50)
print("S3 파일 업로드 시작")
print("=" * 50)
print(f"로컬 파일:{local_file}")
print(f"S3 경로: s3://{BUCKET_NAME}/{s3_file}")
print("-" * 50)

try:
    # s3.upload_file(local_file,BUCKET_NAME, s3_file) # boto3 버전 업데이트 문제로 오류 생겨서 밑의 코드로 대체함. 이 방법이 더 확실하다고 하여, 이걸(코드)로 진행함.
    with open(local_file, 'rb') as f: # 더 안정적임! # 'rb': read binary(바이너리 읽기 모드). 즉, 이진법으로 있는 그대로로 읽음. 더 컴퓨터 친화적인 읽기 방식. 비유를 하자면, 텍스트 모드로 변환하면 변환 과정에서 오류가 생길 수 있으니, 원본(바이너리)을 그대로 보내는 방식임.
        s3.put_object(Bucket=BUCKET_NAME, Key=s3_file, Body=f) # 'put_object': upload_file 같은 고수준이 아닌 저수준에서 내가 직접 파일(오브젝트,객체)을 다루는 것을 의미함.
    print("\n✅ 업로드 성공!")
    print(f"파일 위치: s3://{BUCKET_NAME}/{s3_file}")
except FileNotFoundError: # 파일을 못 찾는 '특정' 에러만 잡음.
    print("\n❌ 오류: 로컬 파일을 찾을 수 없습니다!")
    print("local_file 경로를 확인하세요.")
except Exception as e: # 발생한 에러 타입에 따라 매칭되는 except 블록 실행.
    print(f"\n❌ 오류 발생: {e}")

print("=" * 50)