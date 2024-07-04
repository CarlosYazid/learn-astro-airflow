from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

def ml_task(ml_params):
    print("ML Task")
    print(ml_params)

with DAG('ml_dag', start_date=datetime(2024, 7, 3), schedule_interval='@daily', catchup=False) as dag:
    
    for ml_params in Variable.get("ml_model_parameters", deserialize_json=True)["param"]:
        ml_task_op = PythonOperator(
            task_id=f'ml_task_{ml_params}',
            python_callable=ml_task,
            op_args=[ml_params]
        )
        
        ml_task_op