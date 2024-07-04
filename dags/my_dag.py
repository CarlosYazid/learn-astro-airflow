from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

def _startTask():
    print("Hello World")

def _endTask():
    print("Goodbye World")


with DAG('my_dag', start_date=datetime(2024, 6, 26), schedule_interval='@daily', catchup=False) as dag:
    
    start = PythonOperator(
        task_id='start',
        python_callable=_startTask
    )
    
    end = PythonOperator(
        task_id='end',
        python_callable=_endTask
    )
    
    start >> end