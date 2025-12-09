# S3에 있는 파일 삭제

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

# 3. 삭제할 파일(테스트용 이전 파일)
file_to_delete = 'raw/samsung_from_python.csv'

# 4. 파일 삭제
print("=" * 50)
print("S3 파일 삭제")
print("=" * 50)
print(f"삭제할 파일: {file_to_delete}")
print("-" * 50)

try:
    s3.delete_object(Bucket=BUCKET_NAME, Key=file_to_delete) # 마찬가지로 함수 이름을 그대로 해석하면 된다. file_to_delete 변수를 key로 받는다고 이해하면 된다.
    print("✅ 삭제 성공!")
except Exception as e:
    print(f"❌ 오류: {e}")

print("=" * 50)