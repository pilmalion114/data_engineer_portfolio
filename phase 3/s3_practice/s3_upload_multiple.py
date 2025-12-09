# 여러 파일을 한 번에 S3에 업로드

import boto3
import csv
from datetime import datetime, timedelta
import os

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

# 3. 업로드할 파일 리스트
files_to_upload = [
    r'C:\Users\dc\Desktop\새로운 포트폴리오를 위한 폴더\데이터,AI\포트폴리오용\데이터 엔지니어링\실습\data_engineer_portfolio\phase 2\phase 2-2\Data\samsung_2024-11-28_2025-11-27.csv',
    r'C:\Users\dc\Desktop\새로운 포트폴리오를 위한 폴더\데이터,AI\포트폴리오용\데이터 엔지니어링\실습\data_engineer_portfolio\phase 2\phase 2-3\Data\samsung_transformed.csv',
    r'C:\Users\dc\Desktop\새로운 포트폴리오를 위한 폴더\데이터,AI\포트폴리오용\데이터 엔지니어링\실습\data_engineer_portfolio\phase 2\phase 2-4\Data\sample_with_derived.csv'
]

# 4. 여러 파일 업로드
print("=" * 50)
print("여러 파일 S3 업로드")
print("=" * 50)

## 각 파일마다 날짜별 폴더에 업로드
for i, local_file in enumerate(files_to_upload,1):
    ## 파일이 존재하는지 확인
    if not os.path.exists(local_file):
        print(f"❌ [{i}] 파일 없음: {local_file}")
        continue # 여기서 멈추고, 다음 반복으로 넘어가기(for문의 다음 iteration으로 넘어가기)! 라는 의미이다.

    ## 오늘부터 며칠 전 데이터로 가정
    days_ago = i - 1 # 0일전(오늘), 1일전(어제), 2일전(그제)
    date = datetime.now() - timedelta(days=days_ago) # timedelta는 time+delta = 시간+차이(변화량)을 의미한다. 즉, days_ago만큼의 변화량으로 빼준다는 의미이다.

    ## 요일 변환(구조 동일함.). 다만, today -> date로만 변경됨.
    weekdays_kr = ['월', '화', '수', '목', '금', '토', '일']
    weekday_kr = weekdays_kr[date.weekday()]

    ## S3 경로(구조 동일함.)
    date_path = f"{date.year}/{date.month:02d}/{date.day:02d}_{weekday_kr}"
    s3_file = f'raw/{date_path}/samsung.csv'

    ## 업로드(구조 동일함.)
    try:
        with open(local_file, 'rb') as f:
            s3.put_object(Bucket=BUCKET_NAME, Key=s3_file, Body=f)

        ## 파일 크기 확인
        file_size = os.path.getsize(local_file) / 1024 # KB로 변환.
        print(f"✅ [{i}] {os.path.basename(local_file)}") # 경로에서 파일명('samsung.csv')만 나타냄.
        print(f"    → {s3_file}") # 전체 경로를 나타냄.
        print(f"    크기: {file_size:.2f} KB") # 소수점 둘째자리까지 표현.
        print() # 가독성을 위한 빈 줄
    except Exception as e:
        print(f"❌ [{i}] 오류: {e}")
        print()

print("=" * 50)
print(f"완료! {len(files_to_upload)}개 파일 업로드 시도")
print("=" * 50)

