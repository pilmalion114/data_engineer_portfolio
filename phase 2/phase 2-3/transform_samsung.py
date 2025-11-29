# 'phase 2-2'의 'extract_samsung.py'에서 이어지는 파일입니다.(그 다음 단계)
# transform(가공) 즉, '데이터 전처리' 부분입니다.

###########################################################################################################
## <STEP 1: 데이터 불러오기 및 파생변수 생성> ##

# 1. 현재 데이터 확인
import pandas as pd

## CSV 파일 불러오기
df = pd.read_csv("../phase 2-2/Data/samsung_2024-11-28_2025-11-27.csv",
                 index_col= 'Date',
                 parse_dates= True) # 'parse_dates' -> 내가 기존에 알고 있는 'parsing(나누다.구분하다.)' + '분석하다(분석해서) -> (데이터 타입을)변환하다.'의 의미가 추가된 것. 여기서, 'parse_dates'는 기존의 문자열 형태의 'Dates'를 DateTime 타입으로 변환한다는 의미이다.

#print(type(df['Date'])) # 'Date'는 index이므로, 일반적인 열(column)처럼 출력이 불가능함(인덱스는 칼럼이 아니므로).
print(f"타입은: {type(df.index)}") # 이렇게 df.index를 출력해야함. df.index가 'Datetime'으로 타입 변환되었는지 확인용.

print("=" * 50)
print("현재 데이터 확인")
print("=" * 50)
print(f"총 데이터: {len(df)} rows")
print(f"컬럼: {list(df.columns)}")
print("\n최근 5일:")
print(df.head())

print("\n기본 통계:")
print(df.describe()) # 통계 요약본. 주로, min/max, 25%/50%/75%(오름차순 정렬 기준임. 하위 25%,50%(중앙값),75%(상위 25%)), count(총 갯수(결측치 제외)),mean(평균),std(표준편차)를 보여줌. 


# 2. 파생 변수 생성 
print("=" * 50)
print("Transform 시작: 파생 변수 생성")
print("=" * 50)

## 1. 이동 평균(Moving Average): n 크기만큼의 윈도우를 통해서 한칸씩 이동하면서 각 구간의 평균을 내는 방식. 
df['MA_5'] = df['Close'].rolling(window=5).mean() #5일치
df['MA_20'] = df['Close'].rolling(window=20).mean() # 20일치

## 2. 변동성(최근 20일 종가 변화율의 표준편차) -> 표준편차로 변동성을 확인할 수 있다.
df['Volatility_20'] = df['Change'].rolling(window=20).std()

## 3. 거래량 이동평균
df['Volume_MA_5'] = df['Volume'].rolling(window=5).mean()

## 4. 가격 범위
df['Price_Range'] =  df['High'] - df['Low']
df['Price_Range_Pct'] = (df['High'] - df['Low'])/df['Close'] * 100  # Percentage로 표현.

## 5. 고가/저가 대비 종가 위치 (0~100%)
df['Close_Position'] = (df['Close'] - df['Low']) / (df['High'] - df['Low']) * 100 # 'high'-'low'는 그 날의 최종 범위를 의미함. 'close'-'low'가 클수록 높은 가격에 마무리를, 작을수록 낮은 가격에 마무리를 함을 의미함. 그림을 그려서 보면 직관적이고 쉬움.

print("\n✅ 파생 변수 생성 완료!")
print(f"새로운 컬럼: MA_5, MA_20, Volatility_20, Volume_MA_5, Price_Range, Price_Range_Pct, Close_Position")


# 결과 확인
print("\n최근 5일 데이터 (파생 변수 포함):")
print(df[['Close', 'MA_5', 'MA_20', 'Volatility_20', 'Price_Range_Pct']].tail())

# 결측치 확인 (이동평균 때문에 초반 데이터에 NaN 발생)
print("\n" + "=" * 50)
print("결측치 확인")
print("=" * 50)
print(df.isnull().sum()) # 2차원이므로, 한번만 sum() 했으므로, 각 컬렴별로 Nan(결측치)가 합계돼서 나옴.


###########################################################################################################
## <STEP 2: 데이터 검증 로직 작성> ##

print("\n" + "=" * 50)
print("Step 2: 데이터 검증 로직")
print("=" * 50)

# ========================================
# 1. 논리적 검증
# ========================================

print("\n[1] 논리적 검증") # '\n'으로 한줄 띄어서 '[1] 논리적 검증'을 진행한다는 의미.
print("-" * 50)

## 1-1. High >= Low 체크
invalid_high_low = df[df['High'] < df['Low']] # 유효하지 않는(invalid) high/low는 low > high임을 말하는 것. df['High'] < df['Low']인 df안에 있는 행들을 가져온다는 의미(행으로 동작함).
if len(invalid_high_low) > 0:
    print(f"⚠️ High < Low 오류: {len(invalid_high_low)}건")
    print(invalid_high_low[['High', 'Low']]) # 데이터 타입이 데이터프레임이니 'high,'low' 열의 데이터(행)을 출력한다는 의미.

else:
    print("✅ High >= Low: 정상")


## 1-2. High >= Close >= Low 체크
invalid_close = df[(df['Close'] > df['High']) | (df['Close'] < df['Low'])] # '|'는 or 연산자.
if(len(invalid_close)) > 0:
    print(f"⚠️ Close 범위 오류: {len(invalid_close)}건")
    print(invalid_close[['High', 'Close', 'Low']])
else:
    print("✅ High >= Close >= Low: 정상")


## 1-3. High >= Open >= Low 체크
invalid_open = df[(df['Open'] > df['High']) | (df['Open'] < df['Low'])]
if(len(invalid_open)) > 0:
    print(f"⚠️ Open 범위 오류: {len(invalid_open)}건")
    print(invalid_open[['High', 'Open', 'Low']])
else:
    print("✅ High >= Open >= Low: 정상")


## 1-4. Volume > 0 체크
invalid_volume = df[df['Volume'] <= 0]
if len(invalid_volume) > 0:
    print(f"⚠️ Volume <= 0 오류: {len(invalid_volume)}건")
    print(invalid_volume[['Volume']])
else:
    print("✅ Volume > 0: 정상")


## 1-5. 가격 음수 체크
negative_price = df[(df['Open'] < 0) | (df['High'] < 0) | 
                    (df['Low'] < 0) | (df['Close'] < 0)]
if len(negative_price) > 0:
    print(f"⚠️ 음수 가격: {len(negative_price)}건")
else:
    print("✅ 모든 가격 양수: 정상")


# ========================================
# 2. 이상치 탐지: 이상치란, 말 그대로 '이상한 수치'를 의미한다. 뒤에 코드 보면 나오겠지만, 변동 폭이 큰 값들을 주로 이상치라고 한다. 정확하게 말하자면, '데이터 분포에서 다른 값들과 크게 벗어난 극단값'을 의미한다.
# ========================================

print("\n" + "-" * 50)
print("[2] 이상치 탐지")
print("-" * 50)

## 2-1. 급등/급락 탐지 (변화율 ±5% 이상)
sharp_up = df[df['Change'] >= 5.0]
sharp_down = df[df['Change'] <= -5.0]

print(f"\n급등 (5% 이상): {len(sharp_up)}건")
if len(sharp_up) > 0:
    print(sharp_up[['Close', 'Change']].sort_values('Change',ascending=False).head(3)) # 'sort_values': 정렬 방법. 'Change'를 기준으로 내림차순 정렬. head(3) 맨 앞 3개를 출력.

print(f"\n급락 (5% 이하): {len(sharp_down)}건")
if len(sharp_down) > 0:
    print(sharp_down[['Close', 'Change']].sort_values('Change').head(3))


## 2-2. 거래량 이상치 (평균의 2배 이상)
volume_mean = df['Volume'].mean()
volume_threshold = volume_mean * 2 # 'threshold': 임계값(어떤 기준을 만족하는지 판단하는 경계값). 이 임계값을 기준으로 이 값 이상이면 A, 아니면 B 이렇게 나누는(분기점이 되는) 기준값임. 머신러닝/딥러닝 용어.

high_volume = df[df['Volume'] >= volume_threshold]
print(f"\n거래량 폭증 (평균의 2배 이상): {len(high_volume)}건")
if len(high_volume) > 0:
    print(high_volume[['Volume', 'Change']].sort_values('Volume', ascending=False).head(3))


## 2-3. 가격 범위 이상치 (일중 변동폭 5% 이상)
high_volatility = df[df['Price_Range_Pct'] >= 5.0]
print(f"\n일중 변동폭 큼 (5% 이상): {len(high_volatility)}건")
if len(high_volatility) > 0:
    print(high_volatility[['High', 'Low', 'Price_Range_Pct']].sort_values('Price_Range_Pct', ascending=False).head(3))

# ========================================
# 3. 검증 결과 요약
# ========================================
print("\n" + "=" * 50)
print("검증 결과 요약")
print("=" * 50)

validation_summary = { # 딕셔너리 타입.
    '총 데이터': len(df),
    '논리적 오류': len(invalid_high_low) + len(invalid_close) +
                    len(invalid_open) + len(invalid_volume) + len(negative_price),
    '급등 (5%+)': len(sharp_up),
    '급락 (5%-)': len(sharp_down),
    '거래량 폭증': len(high_volume),
    '일중 변동 큼': len(high_volatility)
}

for key,value in validation_summary.items(): # items()는 말 그대로 validation_summary안에 있는 key,value들을 의미함. (key,value) 쌍을 튜플로 묶어서 반환.
    print(f"{key}: {value:,}건") # ':,' -> 천단위(세자리마다) 구분 콤마.

# 1. 데이터 품질 점수
total_issues = validation_summary['논리적 오류']
quality_score = (1- (total_issues/len(df))) * 100

print(f"\n데이터 품질 점수: {quality_score:.1f}/100")

if quality_score == 100:
    print("✅ 완벽한 데이터 품질!")
elif quality_score >= 95:
    print("✅ 매우 좋은 데이터 품질")
elif quality_score >= 90:
    print("⚠️ 양호한 데이터 품질 (일부 검토 필요)")
else:
    print("❌ 데이터 품질 검토 필요!")

print("\n" + "=" * 50)
print("Step 2: 데이터 검증 완료!")
print("=" * 50)

###########################################################################################################
## <STEP 3: 데이터 csv/txt로 저장> ##
print("\n" + "=" * 50)
print("Step 3: 데이터 저장")
print("=" * 50)

import os
from datetime import datetime

# ========================================
# 3-1. 변환된 데이터 저장 (CSV)
# ========================================
print("\n[1] 변환된 데이터 저장")
print("-" * 50)

os.makedirs("Data", exist_ok=True)

output_file = "Data/samsung_transformed.csv"
df.to_csv(output_file, encoding='utf-8-sig') # 우리가 'step_1: 파생변수 생성'할 때, df에다가 새로운 열을 계속 생성했으므로, 이 csv 파일에는 파생변수 생성한 게 들어간다.

print(f"✅ 파일 저장: {output_file}")
print(f"   - 총 컬럼: {len(df.columns)}개")
print(f"   - 총 데이터: {len(df)}건")
print(f"   - 파일 크기: {os.path.getsize(output_file) / 1024:.1f} KB") # getsize는 byte 단위로 반환하는데, 1024로 나눔으로써 KB(킬로바이트)로 변환한다.


# ========================================
# 3-2. 검증 리포트 저장 (TXT)
# ========================================
print("\n[2] 검증 리포트 저장")
print("-" * 50)

report_file = "Data/validation_report.txt"

with open(report_file, 'w', encoding='utf-8') as f: # 'w': 쓰기 모드(write)
    ## 리포트 헤더
    f.write("=" * 60 + "\n")
    f.write("삼성전자 주가 데이터 Transform & 검증 리포트\n")
    f.write("=" * 60 + "\n")
    f.write(f"생성 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"데이터 기간: {df.index[0]} ~ {df.index[-1]}\n")
    f.write(f"총 데이터: {len(df)}건\n")
    f.write("\n")

    # ========================================
    # Step 1: 파생 변수 생성 결과
    # ========================================

    f.write("=" * 60 + "\n")
    f.write("Step 1: 파생 변수 생성\n")
    f.write("=" * 60 + "\n\n")

    f.write("생성된 파생 변수:\n")
    f.write("-" * 60 + "\n")
    derived_cols = ['MA_5', 'MA_20', 'Volatility_20', 'Volume_MA_5', 
                    'Price_Range', 'Price_Range_Pct', 'Close_Position']
    for i,col in enumerate(derived_cols,1): # 1은 1부터 index(i)가 시작함을 알림. 기본값은 0부터 시작.
        f.write(f"{i}. {col}\n")

    f.write("\n최근 5일 데이터 샘플:\n")
    f.write("-" * 60 + "\n")
    sample_cols = ['Close', 'MA_5', 'MA_20', 'Volatility_20', 'Price_Range_Pct']
    f.write(df[sample_cols].tail().to_string()) # 'to_string()': 문자열로 변환하라는 의미. 하지만, 단순 문자열로 변환이 아닌, Dataframe을 '표 형태로 유지한 문자열'로 변환하라는 의미이다.
    f.write("\n\n")

    f.write("결측치 현황:\n")
    f.write("-" * 60 + "\n")
    null_counts = df.isnull().sum() # 2차원 형태를 한번의 sum()을 통해 1차원으로 만들고, 이는 각 컬럼별로 결측치 갯수를 세게 만듦.
    for col, count in null_counts.items():
        if count > 0:
            f.write(f"{col}: {count}개\n")
    f.write("\n")

    # ========================================
    # Step 2: 데이터 검증 결과
    # ========================================
    f.write("=" * 60 + "\n")
    f.write("Step 2: 데이터 검증 로직\n")
    f.write("=" * 60 + "\n\n")

    # 논리적 검증
    f.write("[1] 논리적 검증\n")
    f.write("-" * 60 + "\n")

    invalid_high_low = df[df['High'] < df['Low']]
    f.write(f"High >= Low: {'정상' if len(invalid_high_low) == 0 else f'오류 {len(invalid_high_low)}건'}\n") # {}을 2개 쓴 이유는 변수를 넣는 거 뿐만 아니라, if-else 연산 때문이기도 함. f-string {}은 '변수(f"{변수}" )','연산(f"{10 + 20}" )','함수 호출(f"{my_function()}")','삼항 연산자(if-else)(f"{'A' if x > 0 else 'B'}")' 등이 가능함. 내가 전에도 f-string은 모든 것이 가능하다고 기재했음.
    
    invalid_close = df[(df['Close'] > df['High']) | (df['Close'] < df['Low'])]
    f.write(f"High >= Close >= Low: {'정상' if len(invalid_close) == 0 else f'오류 {len(invalid_close)}건'}\n")
    
    invalid_open = df[(df['Open'] > df['High']) | (df['Open'] < df['Low'])]
    f.write(f"High >= Open >= Low: {'정상' if len(invalid_open) == 0 else f'오류 {len(invalid_open)}건'}\n")
    
    invalid_volume = df[df['Volume'] <= 0]
    f.write(f"Volume > 0: {'정상' if len(invalid_volume) == 0 else f'오류 {len(invalid_volume)}건'}\n")

    negative_price = df[(df['Open'] < 0) | (df['High'] < 0) | 
                         (df['Low'] < 0) | (df['Close'] < 0)]
    f.write(f"모든 가격 양수: {'정상' if len(negative_price) == 0 else f'오류 {len(negative_price)}건'}\n")
    f.write("\n")

    # 이상치 탐지
    f.write("[2] 이상치 탐지\n")
    f.write("-" * 60 + "\n")

    # 급등/급락
    sharp_up = df[df['Change'] >= 5.0]
    sharp_down = df[df['Change'] <= -5.0]

    f.write(f"\n급등 (5% 이상): {len(sharp_up)}건\n")
    if len(sharp_up) > 0:
        f.write(sharp_up[['Close', 'Change']].sort_values('Change', ascending=False).head(3).to_string())
        f.write("\n")
    
    f.write(f"\n급락 (5% 이하): {len(sharp_down)}건\n")
    if len(sharp_down) > 0:
        f.write(sharp_down[['Close', 'Change']].sort_values('Change').head(3).to_string())
        f.write("\n")

    # 거래량 폭증
    volume_mean = df['Volume'].mean()
    volume_threshold = volume_mean * 2
    high_volume = df[df['Volume'] >= volume_threshold]

    f.write(f"\n거래량 폭증 (평균 {volume_mean:,.0f}의 2배 이상): {len(high_volume)}건\n")
    if len(high_volume) > 0:
        f.write(high_volume[['Volume', 'Change']].sort_values('Volume', ascending=False).head(5).to_string())
        f.write("\n")

    # 일중 변동폭
    high_volatility = df[df['Price_Range_Pct'] >= 5.0]
    f.write(f"\n일중 변동폭 큼 (5% 이상): {len(high_volatility)}건\n")
    if len(high_volatility) > 0:
        f.write(high_volatility[['High', 'Low', 'Price_Range_Pct']].sort_values('Price_Range_Pct', ascending=False).head(5).to_string())
        f.write("\n")

    
    # 검증 결과 요약
    f.write("\n" + "=" * 60 + "\n")
    f.write("검증 결과 요약\n")
    f.write("=" * 60 + "\n")

    total_issues = len(invalid_high_low) + len(invalid_close) + len(invalid_open) + len(invalid_volume) + len(negative_price)
    quality_score = (1 - (total_issues / len(df))) * 100

    f.write(f"총 데이터: {len(df):,}건\n")
    f.write(f"논리적 오류: {total_issues}건\n")
    f.write(f"급등 (5%+): {len(sharp_up)}건\n")
    f.write(f"급락 (5%-): {len(sharp_down)}건\n")
    f.write(f"거래량 폭증: {len(high_volume)}건\n")
    f.write(f"일중 변동 큼: {len(high_volatility)}건\n")
    f.write(f"\n데이터 품질 점수: {quality_score:.1f}/100\n")
    
    if quality_score == 100:
        f.write("✅ 완벽한 데이터 품질!\n")
    elif quality_score >= 95:
        f.write("✅ 매우 좋은 데이터 품질\n")
    elif quality_score >= 90:
        f.write("⚠️ 양호한 데이터 품질 (일부 검토 필요)\n")
    else:
        f.write("❌ 데이터 품질 검토 필요!\n")
    
    f.write("\n" + "=" * 60 + "\n")
    f.write("리포트 생성 완료\n")
    f.write("=" * 60 + "\n")

print(f"✅ 검증 리포트 저장: {report_file}")
print(f"   - 파일 크기: {os.path.getsize(report_file) / 1024:.1f} KB")

# ========================================
# 3-3. 최종 요약
# ========================================
print("\n" + "=" * 50)
print("Transform 완료 요약")
print("=" * 50)
print(f"""
생성된 파일:
1. {output_file}
    - 변환된 주가 데이터 (파생 변수 포함)
    - {len(df.columns)}개 컬럼 × {len(df)}건

2. {report_file}
   - 전체 검증 리포트
   - 파생 변수 생성 + 검증 결과
    
데이터 품질: {quality_score:.1f}/100
상태: ✅ Transform 완료!

""")

print("=" * 50)
print("Phase 2-3 Transform 전체 완료!")
print("=" * 50)
