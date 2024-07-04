from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.models import Variable

with DAG('db_example_dag', start_date=datetime(2024, 7, 3), schedule_interval='@daily', catchup=False) as dag:
    
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_conn',
        sql='''
            CREATE TABLE IF NOT EXISTS users (
                user_id SERIAL PRIMARY KEY,
                name VARCHAR NOT NULL,
                email VARCHAR NOT NULL,
                dateBorn DATE NOT NULL
            );
        '''
    )
    
    id_task = 0
    
    for name, email, dateBorn in Variable.get("json_register", deserialize_json=True)["users"]:
        
        id_task += 1
    
        insert_users = PostgresOperator(
            task_id=f'insert_users_{id_task}',
            postgres_conn_id='postgres_conn',
            sql='''INSERT INTO users (name, email,dateBorn) VALUES ("{name}", "{email}",{dateBorn});'''
        )
    
    
    get_users = PostgresOperator(
        task_id='get_users',
        postgres_conn_id='postgres_conn',
        sql='SELECT * FROM users;'
    )
    
    create_table >> insert_users >> get_users