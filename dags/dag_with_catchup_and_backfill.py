from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

deafult_args = {"owner": "Swapnil", "retries": 5, "retry_dealy": timedelta(minutes=5)}

with DAG(
    dag_id="dag_with_catchup_backfill_v02",
    default_args=deafult_args,
    start_date=datetime(2024, 1, 30),
    schedule_interval="@daily",
    catchup=False
) as dag:
    task1=BashOperator(
        task_id='task1',
        bash_command='echo This is a simple bash command!'
    )