# https://www.thesportsdb.com/documentation -> 'thesportsdb' official DB documentation

import requests

# cf.)영국 리그 검색하기
url_eng_league = "https://www.thesportsdb.com/api/v1/json/3/search_all_leagues.php"
params_eng_league = {"c": "England"}

response_eng_league = requests.get(url_eng_league, params=params_eng_league)
data = response_eng_league.json()

print("=== 영국 축구 리그 목록 ===\n")
if data['countries']:
    for league in data['countries']:
        print(f"리그: {league['strLeague']}")
        print(f"ID: {league['idLeague']}")
        print(f"스포츠: {league['strSport']}")
        print()

# TheSportsDB - 무료, API Key 필요 없음!
print("=== EPL 다음 경기 일정 ===\n")

url = "https://www.thesportsdb.com/api/v1/json/3/eventsnextleague.php"
params = {"id": "4328"}  # EPL League ID

reponse = requests.get(url, params=params) # get method로 url&params 가져오기. params는 url http 요청에서 '?' 쿼리 뒤에 붙는다.

"""
url = "https://example.com/api"
params = {"id": "4328", "season": "2024"}

# 실제로 보내지는 URL:
# https://example.com/api?id=4328&season=2024
"""

data = reponse.json() # 응답은 json 형태(키-값. 딕셔너리 형태)로 받겠다.

events = data['events'] or [] # []: 실제로 EPL 경기가 안 들어있을 경우를 대비해서 []로 둠.

epl_events = [
    m for m in events
    #if m.get('strLeague') == 'English Premier League' 
    if m.get('strLeague') == 'English Premier League' or 'English League Championship'
]

# 다음 5경기 출력
# cf.) 'eventsnextLeague.php는 epl이 아닌 championship(2부 리그) 정보까지 섞여나오는 오류(버그)가 있음. 그래서 구조를 좀 바꿈.
# 현재 여기서는 epl league가 안 나오지만, 'API-Football'을 활용해서 데이터를 가져올 수도 있다. 나중에 해보길.

for i,match in enumerate(epl_events[:5],1): # enumerate 객체: 숫자 번호(i)와 데이터를 묶어서 하나로 만드는 객체.
    print(f"{i}. {match['strHomeTeam']} vs {match['strAwayTeam']}")
    # print(f" 리그: {match['strLeague']}")
    print(f" 날짜: {match['dateEvent']} {match['strTime']}")
    print(f" 경기장: {match['strVenue']}")
    print()




print("\n=== EPL 현재 순위표 ===\n")

# 순위표 가져오기
url2 = "https://www.thesportsdb.com/api/v1/json/3/lookuptable.php"
params2 = {
    "l": "4328", # EPL
    "s": "2025-2026" # 시즌
}

reponse2 = requests.get(url2,params2)
data2 = reponse2.json()

# 상위 10팀 출력
if data2['table']:
    for team in data2['table'][:10]:
        print(f"{team['intRank']}위: {team['strTeam']}")
        print(f" 승점: {team['intPoints']}점 "
              f"({team['intWin']}승 {team['intDraw']}무 {team['intLoss']}패)")
        print(f" 득실차: {team['intGoalsFor']} - {team['intGoalsAgainst']} "
              f"(+{team['intGoalDifference']})")
        print()


# fetch한 데이터들 csv로 저장하기
import pandas as pd
from datetime import datetime
import os

# 1. 경기 일정

## 경기 일정 저장
if epl_events:
    fixtures_list = [] # fixture: 경기 일정.
    for match in epl_events:
        fixtures_list.append({
            '날짜': match['dateEvent'],
            '시간': match.get('strTime', ''), # 'strTime' 키가 있으면 값 반환, 없으면 ''(빈 문자열) 반환.
            '홈팀': match['strHomeTeam'],
            '원정팀': match['strAwayTeam'],
            '경기장': match.get('strVenue', ''),
            '리그': match.get('strLeague', '')
        })

    df_fixtures = pd.DataFrame(fixtures_list) # list -> Dataframe으로 변환.

    # data 폴더 생성
    os.makedirs('data', exist_ok=True) # exist_ok=True: 존재해도 괜찮아(에러 안 날게)

    # 날짜별 파일명
    today = datetime.now().strftime('%Y%m%d')
    fixtures_filename = f'data/epl_fixtures_{today}.csv'

    df_fixtures.to_csv(fixtures_filename, index=False, encoding='utf-8-sig')
    print(f"\n✅ 경기 일정 저장 완료: {fixtures_filename}\n")


# 2. EPL 현재 순위표
## EPL 현재 순위표 저장

standing_list = []
for team in data2['table']:
    standing_list.append({
        '순위': team['intRank'],
        '팀명': team['strTeam'],
        '승점': team['intPoints'],
        '경기수': team['intPlayed'],
        '승': team['intWin'],
        '무': team['intDraw'],
        '패': team['intLoss'],
        '득점': team['intGoalsFor'],
        '실점': team['intGoalsAgainst'],
        '득실차': team['intGoalDifference']
    })

df_standings = pd.DataFrame(standing_list)

# 날짜별 파일명
standings_filename = f'data/epl_standings_{today}.csv'

df_standings.to_csv(standings_filename, index=False, encoding='utf-8-sig')
print(f"\n✅ 순위표 저장 완료: {standings_filename}")
print(f"   총 {len(df_standings)}개 팀 데이터\n")

