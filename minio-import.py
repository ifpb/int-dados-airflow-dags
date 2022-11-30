"""
Example DAG demonstrating the usage of the TaskFlow API to execute Python functions natively and within a
virtual environment.
"""
from __future__ import annotations

import logging
import shutil
import sys
import tempfile
import time
from pprint import pprint

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator
from airflow.operators.bash import BashOperator
import pandas as pd
from minio import Minio
from io import BytesIO
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable

BASE_DIR = tempfile.gettempdir()

minio_connection = BaseHook.get_connection('minio')
host = minio_connection.host + ':' + str(minio_connection.port)

client = Minio(host, secure=False, access_key=minio_connection.login, secret_key=minio_connection.password)

if minio_connection.schema == "https":
    client = Minio(host, secure=True, access_key=minio_connection.login, secret_key=minio_connection.password)

with DAG(
    dag_id="minio_import",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["minio"],
) as dag:
 
    file_download_step = BashOperator(
        task_id='file-download',
        bash_command='wget https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SGL/2020/uf=PB/lote=1/part-00000-0508a0cd-d93a-42d4-b285-50673d466ef5.c000.csv -P /Users/diegopessoa/airflow/downloads/',
    )

    file_rename_step = BashOperator(
        task_id='file-rename',
        bash_command='cd /Users/diegopessoa/airflow/downloads/ && mv part-00000-0508a0cd-d93a-42d4-b285-50673d466ef5.c000.csv sindrome-gripal.csv',
    )

    @task(task_id="print-csv-file")
    def print_csv(ds=None, **kwargs):
        df = pd.read_csv('/Users/diegopessoa/airflow/downloads/sindrome-gripal.csv', sep=";", encoding='utf-8', low_memory=False)
        df.fillna('', inplace=True)
        print(df.head())

    print_csv_file_step = print_csv()

    @task(task_id="check-minio-connection")
    def check_minio_connection(ds=None, **kwargs):
        client.list_buckets()
        print("Conexão ativa!")

    check_minio_connection_step = check_minio_connection()

    @task(task_id="create-minio-buckets")
    def create_minio_buckets(ds=None, **kwargs):
        existing_buckets = client.list_buckets()
        existing_buckets = [i.name for i in existing_buckets]
        if "pendentes" not in existing_buckets:
            client.make_bucket("pendentes")
        if "processados" not in existing_buckets:
            client.make_bucket("processados")
        if "baixados" not in existing_buckets:
            client.make_bucket("baixados")
        print(client.list_buckets())
    create_minio_buckets_step = create_minio_buckets()

    @task(task_id="send-file")
    def send_file(ds=None, **kwargs):
        client.fput_object("pendentes", "sindrome-gripal.csv", "/Users/diegopessoa/airflow/downloads/sindrome-gripal.csv")
        print("Arquivo enviado com sucesso!")
    send_file_step = send_file()

    @task(task_id="download-file")
    def download_file(ds=None, **kwargs):
        arquivo = client.get_object("baixados", "movies.csv")
        df = pd.read_csv(BytesIO(arquivo.read()))
        print(df.head())
    download_file_step = download_file()

file_download_step >> file_rename_step
file_rename_step >> print_csv_file_step
file_rename_step >> check_minio_connection_step
check_minio_connection_step >> create_minio_buckets_step
create_minio_buckets_step >> send_file_step
send_file_step >> download_file_step