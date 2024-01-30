from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {"owner": "Swapnil", "retries": 5, "retry_delay": timedelta(minutes=5)}


@dag(
    dag_id="dag_with_taskflow_api_v02",
    default_args=default_args,
    start_date=datetime(2024, 1, 30),
    schedule_interval="@daily",
)
def hello_world_etl():
    @task(multiple_outputs=True)
    def get_name():
        return {"first_name": "Swapnil", "last_name": "Sonawane"}

    @task
    def get_age():
        return 25

    @task()
    def greet(first_name, last_name, age):
        print(
            f"Hello World! my name is {first_name} {last_name} and I'm {age} years old!"
        )

    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict["first_name"], last_name=name_dict["last_name"], age=age)


greet_dag = hello_world_etl()
