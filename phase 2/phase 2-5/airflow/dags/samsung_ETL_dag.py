# 본격적인 '삼성주식 ETL 파일'의 DAG(자동화) 작업이다.

from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum # 파이썬 날짜/시간 라이브러리. Python 기본 'datetime'보다 시간대(time zone) 처리가 편해서 Airflow에서 많이 쓴다고 함.

# 'modules'에서 함수 import
import sys
sys.path.append('/opt/airflow/modules') # 'modules' 폴더 경로 추가.
from airflow_extract_samsung import extract
from airflow_transform_samsung import transform
from airflow_load_samsung import create_table, load_data

with DAG(
    dag_id = "samsung_etl_dag",
    start_date = pendulum.datetime(2025,12,3, tz="UTC"),
    catchup = False,
    schedule = None,
    tags = ["ETL", "samsung"]
) as dag:
    task_1 = PythonOperator(task_id="extract_task", python_callable=extract) # bash에서는 command를 직접 입력했지만, python은 함수를 직접 호출해서 작동시킨다.
    task_2 = PythonOperator(task_id="transform_task", python_callable=transform, op_args=["/opt/airflow/data/samsung_2024-11-28_2025-11-27.csv"]) # 'transform'과 'load_data'는 파라미터가 필요하므로, 'op_args(operator arguments(연산자 인자): 함수에 전달할 파라미터를 리스트로 넣어줌.)' 추가해야 함.
    # task_3 = PythonOperator(task_id="load_task", python_callable=create_table,load_data) # 'python_callable'은 함수 1개만 받을 수 있다. 따라서, task 2개로 분리해서 작성해야 함.
    task_3 = PythonOperator(task_id="create_table_task", python_callable=create_table)
    task_4 = PythonOperator(task_id="load_task", python_callable=load_data, op_args=["/opt/airflow/data/samsung_2024-11-28_2025-11-27.csv"])

    task_1 >> task_2 >> task_3 >> task_4



