import pendulum

from airflow import DAG
from airflow.decorators import task

import numpy as np

with DAG(
    dag_id="integracao-dados",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["integracao"],
) as dag:

    @task()
    def print_hello():
        print("Hello!")

    @task()
    def print_name():
        print("Integração de Dados!")

    print_hello()
    print_name()