# 이 코드는 claude 도움 없이 내가 직접 구글링해서 작성하는 코드이다.(나의 구글링 능력 평가용) -> 일부 코드 수정 부분만 약간의 claude 도움을 받음. 하지만 전체적인 코드 작성은 내가 함. -> 1,2번만 해당함. 3번부터는 Claude 도움 받음.
# TMDB API 공식 문서(Docs)를 보고 참고했다.(그리고 영어 원문으로 Docs 읽어봄.)

## 1. dotenv import 및 token을 통해 권한 획득하기
## Q. 왜 API KEY가 아니라 TOKEN으로 인증을 할까? -> requests 모듈로 get을 해오면 url에 query('?' 부분) 뒤에 api_key값을 그대로 가져온다. 보안상 노출 문제로 인해 'Bearer Token' 방식으로 HTTP 헤더에 숨겨서 전달하므로 url에 안 보이게 된다.(보안상 훨씬 안전)
import requests
import os # .env용
from dotenv import load_dotenv # .env에 있는 설정값 가져올 때 필요한 라이브러리

url = "https://api.themoviedb.org/3/authentication" # 권한 관련 url

load_dotenv() # .env 파일 로드(현재 디렉토리 및 상위 디렉토리 탐색)

token = os.getenv("TMDB_API_TOKEN") # .env에 있는 token 가져오기

headers = {
    "accept": "application/json", # 서버에게 응답을 JSON 형식으로 보내달라고 요청.(요청 헤더)
    "Authorization": f"Bearer {token}" # f-string으로 Bearer 추가.(공식 TMDB api_key 삽입 방식)
}

response = requests.get(url,headers=headers)

print(response.text)


## 2. 현재 인기 영화 가져오기

popular_movie_url = "https://api.themoviedb.org/3/movie/popular?" # Popular movie list url. # 공식 문서에서는 이 url에 params를 다 추가했으나, 본인은 가독성을 위해 분리함.

popular_params = {
    "language": "Ko-KR", # 언어: 한국어
    "page": 1 # 1페이지만 가져오기
}

popular_response = requests.get(popular_movie_url,headers=headers,params=popular_params)
popular_data = popular_response.json()

#print(popular_data) # json 형식 미리 출력
## results 리스트의 각 영화에서 'title'만 추출 
for movie in popular_data['results']:
    print(movie['title'])








