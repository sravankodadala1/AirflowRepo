from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 26),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Assignment5',
    default_args=default_args,
    schedule_interval='@daily',
)

# Define tasks
def even_numbers():
    l = [1, 2, 3, 4, 5]
    even = [i for i in l if i % 2 == 0]
    return even


task1 = PythonOperator(
    task_id="my_python",
    python_callable=even_numbers,
    dag=dag,
)

def task2():
    result = "executing the second task"
    return result


task2 = PythonOperator(
    task_id='task2',
    python_callable=task2,
    retries=3,
    retry_delay=timedelta(minutes=5),
    trigger_rule='all_success',  #Downstream task executes only if all its upstream tasks succeed
    dag=dag,
)

#task dependencies
task1.set_downstream(task2)


