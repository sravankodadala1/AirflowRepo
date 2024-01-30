from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 28),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Assignment1',
    default_args=default_args,
    description='Python Operator',
    schedule_interval=timedelta(days=2),
)

def task1():
    return "Executing the first task"

def task2():
    return "Executing the second task"


task_1 = PythonOperator(
    task_id='task1',
    python_callable=task1,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='task2',
    python_callable=task2,
    dag=dag,
)

#task dependencies
task_1 >> task_2

