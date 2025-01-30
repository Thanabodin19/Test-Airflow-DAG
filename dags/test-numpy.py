from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import numpy as np

def print_hello():
    x = 2
    y = 10
    print(f"result X^Y= {np.power(x,y)}")

dag = DAG('hello_world_test2', description='Hello World DAG2',
          schedule_interval='0 12 * * *',
          start_date=datetime(2024, 12, 12), catchup=False)

hello_operator = PythonOperator(task_id='hello_k8s', python_callable=print_hello, dag=dag)

print(hello_operator)
