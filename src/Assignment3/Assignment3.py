from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 2, 15),
    'retries': 3,
    'retries_delay': timedelta(minutes=5)
}

def even_numbers():
    l = [1, 2, 3, 4, 5]
    even = [i for i in l if i % 2 == 0]
    print(even)


with DAG(
        default_args=default_args,
        dag_id="PythonOperator",
        description="this is the assignment for python operator",
        start_date=datetime(2023, 7, 12, 22),
        schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id="my_pyhton",
        python_callable=even_numbers,
        dag=dag
    )

    task1