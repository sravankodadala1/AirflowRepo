from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

parent_dag_default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 12),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

parent_dag = DAG(
    'parent_dag',
    default_args=parent_dag_default_args,
    description='A parent DAG with Python tasks and TriggerDagRunOperator',
    schedule_interval=timedelta(days=1),
)

trigger_operator = TriggerDagRunOperator(
    task_id='trigger_child_dag',
    #external dag to be triggered
    trigger_dag_id='child_dag',
    dag=parent_dag,
)

def task1(task):
    print(f"This is the {task}")

def task2(task):
    print(f"This is the {task}")

task1 = PythonOperator(
    task_id='task1',
    python_callable=task1,
    op_kwargs={'task':'task1'},
    dag=parent_dag,
)

task2 = PythonOperator(
    task_id='task2',
    python_callable=task2,
    op_kwargs={'task':'task2'},
    dag=parent_dag,
)

trigger_operator >> [task1, task2]
