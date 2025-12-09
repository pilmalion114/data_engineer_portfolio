# 날짜별 폴더 구조로 S3에 업로드

import boto3
import csv
from datetime import datetime

# 1. CSV에서 AWS 자격증명 읽기(upload.py/download.py와 동일함.)
csv_file = 'boto3-user_accessKeys.csv'

with open(csv_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    credentials = next(reader)

AWS_ACCESS_KEY_ID = credentials['Access key ID']
AWS_SECRET_ACCESS_KEY = credentials['Secret access key']
AWS_REGION = 'ap-northeast-2'

# 2. S3 클라이언트 생성
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# 3. 날짜별 폴더 경로 생성
today = datetime.now()

## 요일을 한글로 변환. weekday()는 숫자(0~6)로 반환됨. 0은 월,... 6은 일요일이다.
weekdays_kr = ['월', '화', '수', '목', '금', '토', '일'] # 리스트 순서대로 weekday()의 인덱스 숫자와 동일하게 매칭되어 적용된다.
weekday_kr = weekdays_kr[today.weekday()]

date_path = f"{today.year}/{today.month:02d}/{today.day:02d}_{weekday_kr}" # 02d를 붙이는 이유: 02d의 의미는 무조건 두자리로 만드는 문법이며, 한 자리일 경우 앞에 0을 붙이라는 의미이다. 이는 '오름차순' 정렬을 위해서 쓴다. # 또한, 컴퓨터는 앞에서부터 비교하기 때문에, 요일을 넣어도 오름차순 정렬이 유지된다.

# 4. S3 경로
BUCKET_NAME = 'samsung-stock-data-pilmalion114'
local_file = r'C:\Users\dc\Desktop\새로운 포트폴리오를 위한 폴더\데이터,AI\포트폴리오용\데이터 엔지니어링\실습\data_engineer_portfolio\phase 2\phase 2-2\Data\samsung_2024-11-28_2025-11-27.csv'
s3_file = f'raw/{date_path}/samsung.csv'

# 5. 업로드(upload.py와 동일함.)
print("=" * 50)
print("날짜별 폴더로 S3 업로드")
print("=" * 50)
print(f"날짜: {today.year}-{today.month:02d}-{today.day:02d} ({weekday_kr})")
print(f"S3 경로: {s3_file}")
print("-" * 50)

try:
    with open(local_file, 'rb') as f:
        s3.put_object(Bucket=BUCKET_NAME, Key=s3_file, Body=f)
    print("\n✅ 업로드 성공!")
except FileNotFoundError:
    print(f"\n❌ 파일 없음: {local_file}")
except Exception as e:
    print(f"\n❌ 오류: {e}")

print("=" * 50)

