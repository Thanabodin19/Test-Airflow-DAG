from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def print_hello():
    print('Hello world from first Airflow DAG!')

dag = DAG('hello_world_test', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2024, 12, 12), catchup=False)

hello_operator = PythonOperator(task_id='hello_k8s', python_callable=print_hello, dag=dag)

print(hello_operator)
