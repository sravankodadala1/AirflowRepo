from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

child_dag_default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 12),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

child_dag = DAG(
    'child_dag',
    default_args=child_dag_default_args,
    description='This is a child dag',
    schedule_interval=None,
)

def task1_child(task):
    print(f"This is the {task}")

task1 = PythonOperator(
    task_id='child_task',
    python_callable=task1_child,
    op_kwargs={'task':'child_task'},
    dag=child_dag,
)

