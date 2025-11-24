# 터미널에서 라이브러리 다운로드
# pip install yfinance

"""
import yfinance as yf
import pandas as pd
"""

# SSL 검증 비활성화 (테스트용!) -> yfinance가 내부적으로 curl_cffi를 써서 ssl 우회가 안 먹임.
# curl_cffi: curl(인터넷에서 데이터 스크래핑하는 도구(명령어)) +  cffi(c언어로 만든 것을 python에서 쓸 수 있게 하는 다리). 즉, curl을 python에서 쓸 수 있게 만든 라이브러리.
# 자세한 개념은 나중에 찾아보기. 일단은, requests보다 더 빠르고 안정적이라고 함. 브라우저처럼 행동(anti-bot 우회)


"""
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
"""


"""
# 삼성전자
samsung = yf.Ticker("005930.ks") # 해당 주식의 모든 데이터를 가져올 수 있는 인터페이스 객체(yfinance 라이브러리가 제공함). 주식의 'Ticker Symbol', '고유 식별 코드'에서 따온 것으로 보임.
data = samsung.history(period="5d")
print(type(data))

print("=== 삼성전자 최근 5일 ===")
print(data['Close', 'Volume'])

# 전일 대비 변화율
#data['Change'] = 

"""

import FinanceDataReader as fdr

# 삼성전자
samsung = fdr.DataReader('005930', '2025-11-01', '2025-11-25')
print(type(samsung)) # Dataframe

print("=== 삼성전자 최근 데이터 ===")
print(samsung.tail(10)) # 꼬리. head()의 반대. default는 마지막 5개 출력.

# 전일 대비 변화율
samsung['Change'] = samsung['Close'].pct_change()*100 # Samsung['Change']라는 새로운 열(속성) 추가. 
# 'pct_change': 데이터의 현재값과 이전값 간의 백분율 변화를 계산하는 함수. 퍼센트 체인지임.
print("\n=== 변화율 ===")
print(samsung[['Close','Change']].tail(10))


## csv 파일로 데이터 저장하기
# data 폴더 생성
import pandas as pd
from datetime import datetime
import os

os.makedirs('data',exist_ok=True)

# 날짜별 파일명 (오늘 날짜)
today = datetime.now().strftime('%Y%m%d')
filename = f'data/samsung_stock_{today}.csv'

# CSV 저장
samsung.to_csv(filename, encoding='utf-8-sig')

print(f"\n✅ 주식 데이터 저장 완료: {filename}")
print(f"   데이터 기간: {samsung.index[0].strftime('%Y-%m-%d')} ~ {samsung.index[-1].strftime('%Y-%m-%d')}") # 0번째 인덱스(맨 처음) ~ -1번째 인덱스(맨 마지막)의 데이터를 의미함.
print(f"   총 {len(samsung)}일 데이터")


# ※cf.) 'SSL 인증서 오류가 난다면' -> 난 4번으로 함. 3번은 안 해봤고, 1,2번은 'curl(ssl)' 오류남.
"""
1. 간단한 우회 방법: 이 방법은 테스트용임. 나중에 실제 프로젝트에선 제대로 고쳐야 함.

import yfinance as yf
import pandas as pd

# SSL 검증 비활성화 (테스트용!)
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# 이제 실행
samsung = yf.Ticker("005930.KS")
data = samsung.history(period="5d")

print("=== 삼성전자 최근 5일 ===")
print(data[['Close', 'Volume']])


2. certifi 재설치

pip uninstall certifi
pip install certifi
pip install --upgrade yfinance


3. requests로 직접 호출: yfinance 말고 yahoo finance에서 가져오기

import requests
import pandas as pd
from datetime import datetime, timedelta

# SSL 검증 건너뛰기
symbol = "005930.KS"
end_date = datetime.now()
start_date = end_date - timedelta(days=5)

url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}"
params = {
    'period1': int(start_date.timestamp()),
    'period2': int(end_date.timestamp()),
    'interval': '1d',
    'events': 'history'
}

response = requests.get(url, params=params, verify=False)
print(response.text)  # CSV 형태로 출력됨


4. finance-datareader(한국 주식 특화) 라이브러리 다운로드
pip install finance-datareader

# finance_test3.py
import FinanceDataReader as fdr

# 삼성전자
samsung = fdr.DataReader('005930', '2024-11-01', '2024-11-25')

print("=== 삼성전자 최근 데이터 ===")
print(samsung.tail())

# 전일 대비 변화율
samsung['Change'] = samsung['Close'].pct_change() * 100
print("\n=== 변화율 ===")
print(samsung[['Close', 'Change']].tail())
"""