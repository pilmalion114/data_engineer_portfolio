## models/ 폴더에 있는 각 .sql 모델링 파일들 결과 표를 docker에 tmp/ 폴더를 만들어서 거기에 저장하는 bash 파일 코드.
## .sh = shell script. 쉘 명령어들 모아놓은 파일. bash로 실행하는 스크립트. 

#!/bin/bash 
## 위 명령어는 필수이다. 특수한 경우이며, #!는 셔뱅(shebang), /bin/bash: 이 스크립트를 bash로 실행하라는 의미.

TABLES=( ## 배열(리스트)로 선언
  "dim_movie"
  "dim_genre"
  "dim_date"
  "dim_user"
  "movie_genre"
  "fact_viewlog"
  "movie_with_genre"
)

for table in "${TABLES[@]}"
do
    docker exec movie_postgres psql -U movie_user -d movie_dw -c "\copy (SELECT * FROM public_dbt_models.$table) TO '/tmp/${table}.csv' WITH CSV HEADER" 
    ## docker exec movie_postgres: 'movie_postgres' 컨테이너 안에서 실행.
    ## psql -U movie_user -d movie_dw: -U movie_user = 사용자명, -d movie_dw = 데이터베이스명.
    ## -c "명령어"(-c 옵션): SQL 명령을 직접 실행. (접속 후 바로 실행하고 나가기)
    ## \copy (SELECT * FROM public_dbt_models.$table) TO '/tmp/${table}.csv' WITH CSV HEADER: $table, ${table} 부분은 변수 삽입 부분. 한 마디로 해석하면, SQL 쿼리문 실행한 결과(전부 다 조회하기)를 tmp/ 폴더에 ${table}.csv라는 파일로 복사하라는 의미임.

    docker cp movie_postgres:/tmp/${table}.csv ./models_results/
    ## docker container 안에 있는 .csv 파일들을 로컬의 ./models_results/에 cp(copy)하라는 의미이다.

    echo "✓ $table exported"
    ## echo는 print(출력문)라고 이해하면 됨. 즉, 진행상황 출력하는 코드이다.
done 
