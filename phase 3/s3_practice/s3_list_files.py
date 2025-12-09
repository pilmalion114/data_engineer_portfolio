# s3 버킷의 파일 목록 조회

import boto3
import csv

# 1. AWS 자격증명(동일함.)
csv_file = 'boto3-user_accessKeys.csv'
with open(csv_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    credentials = next(reader)
    
AWS_ACCESS_KEY_ID = credentials['Access key ID']
AWS_SECRET_ACCESS_KEY = credentials['Secret access key']
AWS_REGION = 'ap-northeast-2'

# 2. S3 클라이언트(동일함.)
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

BUCKET_NAME = 'samsung-stock-data-pilmalion114'

# 3. 모든 파일 목록 조회
print("=" * 50)
print("S3 버킷 전체 파일 목록")
print("=" * 50)

response = s3.list_objects_v2(Bucket=BUCKET_NAME) # s3 버킷에 있는 파일들(객체들) 가져오는 함수의 버전 2.

if 'Contents' in response: # response에 'Contents' 키가 있는지 확인 (파일이 있으면 이 키가 존재)
    print(f"총 {len(response['Contents'])}개 파일\n") # response['Contents'] 리스트의 길이 = 파일 개수

    for obj in response['Contents']: # Contents 리스트의 각 딕셔너리(파일 정보)를 obj에 할당
        print(f"📁 {obj['Key']}") # obj 딕셔너리의 'Key' 값 = 파일 경로
        print(f"   크기: {obj['Size'] / 1024:.2f} KB") # obj의 'Size'를 KB로 변환 (바이트 → KB)
        print(f"   수정: {obj['LastModified']}") # obj의 'LastModified' = 마지막 수정 시간
        print()
else:
    print("파일이 없습니다.")

#"""
#response = {
#    'Contents': [
#        {
#            'Key': 'raw/2025/12/09_화/samsung.csv',
#            'Size': 15872,
#            'LastModified': datetime(2025, 12, 9, ...)
#        },
#        {
#            'Key': 'raw/samsung_from_python.csv',
#            'Size': 15872,
#            'LastModified': datetime(2025, 12, 8, ...)
#        }
#    ]
#}
#
#response 구조는 이렇게 생겼다고 함. 'Contents'라는 리스트 안에, 각각의 파일들이 딕셔너리 형태로 일관된 형식으로 존재함. 그래서 리스트의 길이 = 딕셔너리 묶음의 갯수 = 파일 갯수.
#"""

print("=" * 50)

# 4. 특정 폴더만 조회
print("\n특정 폴더 조회: raw/2025/12/")
print("=" * 50)

response_2 = s3.list_objects_v2(
    Bucket = BUCKET_NAME,
    Prefix = 'raw/2025/12' # 이 경로로 시작하는 파일만 조회. # Prefix: 접두사. 단어 앞에 붙어 의미를 더해주는 말.
)

## 동일한 구조
if 'Contents' in response_2:
    print(f"총 {len(response_2['Contents'])}개 파일\n")
    for obj in response_2['Contents']:
        print(f"📁 {obj['Key']}")
else:
    print("파일이 없습니다.")

print("=" * 50)

