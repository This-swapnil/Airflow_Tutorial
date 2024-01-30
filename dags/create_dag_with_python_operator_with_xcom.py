from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {"owner": "swapnil", "retries": 5, "retry_delay": timedelta(minutes=5)}


def greet(ti):
    first_name = ti.xcom_pull(task_ids="get_name", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_name", key="last_name")
    age = ti.xcom_pull(task_ids="get_age", key="age")
    print(f"Hello World my name is {first_name} {last_name} and I'm {age} years old!")


def get_name(ti):
    ti.xcom_push(key="first_name", value="Swapnil")
    ti.xcom_push(key="last_name", value="Sonawane")


def get_age(ti):
    ti.xcom_push(key="age", value=25)


with DAG(
    default_args=default_args,
    dag_id="dag_with_python_operator_iwth_Xcom_v4",
    description="Our first dag using python operator",
    start_date=datetime(2024, 1, 30),
    schedule_interval="@daily",
) as dag:
    task1 = PythonOperator(task_id="greet", python_callable=greet)

    task2 = PythonOperator(task_id="get_name", python_callable=get_name)

    task3 = PythonOperator(task_id="get_age", python_callable=get_age)

    [task3, task2] >> task1
