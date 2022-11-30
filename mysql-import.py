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
import pymysql

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator
from airflow.operators.bash import BashOperator
import pandas as pd
from minio import Minio
from io import BytesIO
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable

BASE_DIR = tempfile.gettempdir()

minio_connection = BaseHook.get_connection('minio')
host = minio_connection.host + ':' + str(minio_connection.port)

client = Minio(host, secure=False, access_key=minio_connection.login, secret_key=minio_connection.password)

if minio_connection.schema == "https":
    client = Minio(host, secure=True, access_key=minio_connection.login, secret_key=minio_connection.password)


df = None

with DAG(
    dag_id="mysql_import",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["mysql"],
) as dag:
 
    @task(task_id="read-minio-file")
    def read_minio_file(ds=None, **kwargs):
        arquivo = client.get_object("baixados", "movies.csv")
        df = pd.read_csv(BytesIO(arquivo.read()))
        print(df.head())

    read_minio_file_step = read_minio_file()

    @task(task_id="write-file-to-mysql")
    def write_file_to_mysql(ds=None, **kwargs):
        arquivo = client.get_object("baixados", "movies.csv")
        df = pd.read_csv(BytesIO(arquivo.read()))

        mysql_connection = BaseHook.get_connection('mysql')

        cnx = create_engine(mysql_connection.schema+'://'+mysql_connection.login+':'+mysql_connection.password+'@'+mysql_connection.host)
        df.to_sql(name='movies', con=cnx, if_exists='replace')
        print("Dados salvos!")

    write_file_to_mysql_step = write_file_to_mysql()

read_minio_file_step >> write_file_to_mysql_step