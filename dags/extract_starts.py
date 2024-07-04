from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime
from airflow.models import Variable
from airflow.decorators import task

with DAG('extract_starts', start_date=datetime(2024, 7, 3), schedule_interval='@daily', catchup=False) as dag:

    def get_github_info(endpoint, token):
        http_hook = HttpHook(method='GET', http_conn_id="github_api")
        response = http_hook.run(endpoint=endpoint, headers={"Authorization": f"Bearer {token}"})
        return response.json()

#Variable.get("github_endpoint")
#Variable.get('github_token')

    @task()
    def task_get_info_spark_repo(ti=None):
        info = get_github_info(Variable.get("github_endpoint_spark"), Variable.get('github_token'))
        ti.xcom_push(key = "info_repo",value = info)
        
    @task()
    def task_get_info_airflow_repo(ti=None):
        info = get_github_info(Variable.get("github_endpoint_airflow"), Variable.get('github_token'))
        ti.xcom_push(key = "info_repo",value = info)

    @task()
    def get_github_stargazers(ti=None):
        info = ti.xcom_pull(key = "info_repo",task_ids = ["task_get_info_spark_repo","task_get_info_airflow_repo"])
        ti.xcom_push(key = "stargazers_count",value = info["stargazers_count"])

    @task()
    def print_stargazers_count(ti=None):
        stargazers_count = ti.xcom_pull(key = "stargazers_count",task_ids = "get_github_stargazers")
        print(f"El número de estrellas del repositorio es: {stargazers_count}")
    
    # Definir la secuencia de las tareas
    task_get_info_spark_repo() >> task_get_info_airflow_repo() >> get_github_stargazers() >> print_stargazers_count()
