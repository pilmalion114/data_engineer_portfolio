# Phase 3-5 ('AWS Athena: S3의 CSV 파일을 SQL로 쿼리할 수 있게 해주는 서버리스 쿼리 서비스')에서 
# BOM Encoding 문제로 인해 Glue Crawler가 CSV 헤더를 제대로 인식하지 못해 Athena 쿼리가 실패.
# 해결: encoding='utf-8'로 설정하여 BOM 없는 새로운 CSV 파일 생성 후 재업로드.

# 아.. 우리가 전에 'utf-8-sig'로 저장해서 BOM이 생겼나봄.. -> 이것도 내가 따로 첨부한 png 사진 참고하기!
# 내가 따로 첨부한 'utf-8' vs 'utf-8-sig'.png 참고하기!

import pandas as pd
import FinanceDataReader as fdr

# 1. 데이터 새로 가져오기
df = fdr.DataReader('005930', '2024-11-28', '2025-11-27') # 삼성전자 주식 데이터 원본, 날짜는 예전거랑 동일하게
df = df.reset_index() # reset_index(): index에 있던 Date 칼럼(열)을 일반 칼럼(열)로 변환해주는 함수. 그럼 변경된 인덱스는 숫자로 채워짐(auto_increment처럼, default는 0부터 채워짐.).

# 2. BOM 없이 저장하기!
df.to_csv('samsung_clean.csv',encoding='utf-8',index=False) # 따로 Data 폴더 안 만들고 그냥 해당 폴더(현재 폴더)에 저장함. index=False는 그 숫자로 채워지는 인덱스를 CSV에 저장하지 않겠다는 의미. 즉, 그냥 index 부분(여기서는 숫자로 채워지지만, 만약 Date를 인덱스로 썼더라도)을 csv에 저장하지 않겠다는 의미이다. 

# 3. (테스트) 정말 BOM이 없는지 확인하는 코드
## 첫 줄만 확인
with open('./samsung_clean.csv','r',encoding='utf-8') as f:
    first_line = f.readline() # readline(): 당연히 유추 가능하겠지만, '파일에서 한 줄씩 읽기'를 실행하는 코드.
    print(repr(first_line)) # repr(): representation의 줄임말. 즉, '표현' 보여주기(일반 print()보다 더 세세하게 보여준다고 이해하면 됨.). repr()을 사용함으로써 숨겨진 문자를 확인할 수 있다.(예를 들어 공백 문자나 줄 바꿈 문자 같은 일반 print에는 직접적으로 안 보이는 문자들). repr()을 쓰면 만약 BOM이 있다면, '\ufeffDate,Open' 이렇게 나온다고 함.

    