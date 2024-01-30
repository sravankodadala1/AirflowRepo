from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 28),
    'retries': 3,
    'retries_delay': timedelta(minutes=5)
}

def even_numbers():
    l = [1, 2, 3, 4, 5]
    even = [i for i in l if i % 2 == 0]
    print(even)


dag_with_cron_expression = DAG(
    'cron_expression',
    default_args=default_args,
    description='scheduling with cron expression',
    schedule_interval='0 0 * * *',  #scheduling everyday at midnight using cron expression
)

dag_with_interval_based = DAG(
    'interval_based_scheduling',
    default_args=default_args,
    description='scheduling with interval_based',
    schedule_interval=timedelta(hours=1),  #scheduling using interval based scheduling
)

task1 = PythonOperator(
    task_id="python_operator",
    python_callable=even_numbers,
    dag=dag_with_cron
)

task2 = PythonOperator(
    task_id="my_python",
    python_callable=even_numbers,
    dag=dag_with_interval
)

task1

