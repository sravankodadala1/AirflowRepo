

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 2, 15),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Assignment2',
    default_args=default_args,
    description='Bash Operator',
    schedule_interval=timedelta(days=1)
)


bash_task = BashOperator(
    task_id='bash_task_execute_script',
    bash_command='echo $((1 + RANDOM % 100))',

    dag=dag
)

bash_task