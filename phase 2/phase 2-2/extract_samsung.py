# 최근 1년치 거래량(240개치) 추출하는 코드입니다.
# 'phase 2-1/finance_test.py'에서 사용했던 'FinanceDataReader' 라이브러리를 사용할 예정입니다.

import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime

# 1. 추출 범위 설정
Start_Date = '2024-11-28'
End_Date = '2025-11-27'
Stock_Code = '005930' # 삼성전자 코드

print("=" * 50)
print("삼성전자 주가 데이터 추출")
print("=" * 50)
print(f"종목 코드: {Stock_Code}")
print(f"추출 기간: {Start_Date} ~ {End_Date}")
print("-" * 50)


# 2. 데이터 추출
print("\n데이터 추출 중...")
samsung = fdr.DataReader(Stock_Code, Start_Date, End_Date)


# 3. 기본 정보 확인 
print(f"\n✅ 추출 완료!")
print(f"총 데이터: {len(samsung)} rows")
print(f"컬럼: {list(samsung.columns)}") # 삼성 데이터프레임의 column들을 list 형식으로 보여 줌.


# 4. 기본 통계
print("\n" + "=" * 50)
print("기본 통계")
print("=" * 50)
print(f"시작일: {samsung.index[0]}") # 첫 인덱스(행). 즉, 시작일 데이터.
print(f"종료일: {samsung.index[-1]}") # 마지막 인덱스(행). 즉, 마지막일 데이터.
print(f"최고가: {samsung['High'].max():,.0f}원") # samsung의 high column에서의 max. 즉, high 중에서도 최고값. 그리고 쉼표(,)는 천 단위 구분 기호를, 0f는 소수점 아래 0자리의 float을, ':'은 포맷 시작(이렇게 설정을 하겠다라는 의미)이다.
print(f"최저가: {samsung['Low'].min():,.0f}원") # samsung의 low column에서의 min. 즉, low 중에서도 최저값.
print(f"평균 거래량: {samsung['Volume'].mean():,.0f}주")

# 최근 5일 데이터 확인
print("\n" + "=" * 50)
print("최근 5일 데이터")
print("=" * 50)
print(samsung.tail())

# 5. 데이터 품질 체크
print("\n" + "=" * 50)
print("데이터 품질 체크")
print("=" * 50)
print(f"결측치: {samsung.isnull().sum().sum()}개") # isnull()을 통해, 데이터프레임의 각 셀에 결측치가 있는지 판단(있으면 true, 없으면 false 반환). 첫번째 sum()을 통해 각 컬럼별로 결측치 갯수를 합함. 두번째 sum()을 통해 전부 다 합하여, 전체 결측치 갯수로 합함.(2차원)
print(f"중복: {samsung.duplicated().sum()}개") # 이거는 행 단위이므로, duplicated()를 통해 중복행이 있는지 판단하고, 한번의 sum()을 통해 전체로 합침.(1차원)

print("\n" + "=" * 50)
print("추출 완료!")
print("=" * 50)

# 6. 최종 CSV 저장
import os

os.makedirs("Data", exist_ok=True) 

filename = f"Data/samsung_{Start_Date}_{End_Date}.csv"
samsung.to_csv(filename, encoding='utf-8-sig')
print(f"\n✅ 저장 완료: {filename}")