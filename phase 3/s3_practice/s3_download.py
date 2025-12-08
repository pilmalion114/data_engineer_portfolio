# S3에서 파일 다운로드하는 스크립트

import boto3
import csv


# 1. CSV에서 자격증명 읽기!(upload와 동일함.)
csv_file = 'boto3-user_accessKeys.csv'

with open(csv_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    credentials = next(reader)
    
AWS_ACCESS_KEY_ID = credentials['Access key ID']
AWS_SECRET_ACCESS_KEY = credentials['Secret access key']
AWS_REGION = 'ap-northeast-2'

# 2. S3 클라이언트 생성(upload와 동일함.)
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# 3. Bucket_Name, s3_file, local_file 설정.
BUCKET_NAME = 'samsung-stock-data-pilmalion114'
s3_file = 'raw/samsung_from_python.csv'  # S3에 있는 파일

import os
os.makedirs("Data", exist_ok=True)
local_file = 'Data/downloaded_samsung.csv'  # 로컬에 저장될 파일명

# 4. 다운로드 실행(upload와 구조 동일함.)
print("=" * 50)
print("S3 파일 다운로드 시작")
print("=" * 50)
print(f"S3 경로: s3://{BUCKET_NAME}/{s3_file}")
print(f"저장 위치: {local_file}")
print("-" * 50)

try:
    s3.download_file(BUCKET_NAME, s3_file, local_file)
    print("\n✅ 다운로드 성공!")
    print(f"파일 저장: {local_file}")
except Exception as e:
    print(f"\n❌ 오류 발생: {e}")

print("=" * 50)
