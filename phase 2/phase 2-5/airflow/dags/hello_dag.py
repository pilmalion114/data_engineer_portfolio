from __future__ import annotations
# 밑에 3개 import한 거 노란줄 뜨는데, 따로 터미널로 라이브러리 다운로드 안 해도 된다. 이 파이썬 코드는 Docker 컨테이너 안에서 실행되므로, airflow 잘 설치됐다면 알아서 찾아서 실행해준다.
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG( # 설정(Configuration)하는 부분.
    dag_id = "simple_bash_dag",
    start_date = pendulum.datetime(2025,12,3, tz="UTC"), # tz: time zone(시간대)
    catchup = False, # 과거 실행 안 된 것들도 포함해서 실행할지 여부. True: start_date부터 오늘까지 밀린 거 다 실행. False면 지금부터만 실행. 보통은 False로 한다고 함.(안 그러면 갑자기 수백 개 실행되므로)
    schedule = None, # 자동 실행 주기. None: 수동 실행만(자동 스케쥴 없음), "@daily": 매일, "0 9 * * *": 매일 오전 9시(cron 표현식)(cron 표현식은 공백으로 구분해야한다고 함.)(0(분) 9(시) *(일) *(월) *(요일))(*는 마찬가지로 '모든(all)'을 의미함.)
    tags = ['example'] # 이름표,꼬리표 같은 거. tags=["example"] # 예제용, tags=["ETL", "주식"] # ETL이면서 주식 관련. Airflow UI에서 필터링하기 편하기 위함.
) as dag:
    task_1 = BashOperator(task_id="task_1", bash_command="echo 'Hello World'") # Bash(리눅스 기반 shell(쉘)) 기반 Operator(설계도)로 task를 만드는 과정. 'bash_command'가 shell 명령어이다. 
    # cf.) shell: 명령어 실행하는 주체. terminal: 껍데기.shell을 보여주는 창.
    task_2 = BashOperator(task_id="task_2", bash_command="echo 'Hello Airflow'")

    task_1 >> task_2 # task_1 실행하고 task_2 실행하라는 의미. 즉, 순서대로 실행해라. 마치 동기(sync)식(순서가 있는) 절차 같음.