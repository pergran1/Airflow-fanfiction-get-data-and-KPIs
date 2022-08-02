# Eftersom man inte kan debugga allt i airflow lokalt så börjar jag med att ladda in funktionera

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.settings import AIRFLOW_HOME
import sys
# sys.path.append('/opt/airflow/dags/functions_folder/functions_file')
import sys
import os
# sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
import requests
# from more_itertools import sliced
import re
import numpy as np
import pandas as pd
import time
import json
# from nltk.tok enize import sent_tokenize
# from nltk import *
from itertools import chain
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2
import time
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname('functions_file.py'),
                                                os.path.pardir)))  # detta funkar för att få functioner från en fil
from functions_file import *

TABLE_NAME = "kpi_fanfictions"
SQL_PATCH = Variable.get('sql_path')


def get_start():
    return "The dag has started"


default_args = {
    'owner': 'Per Granberg',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        dag_id="Fanfic_dag",
        default_args=default_args,
        description="Collect KPIs for fanfictions",
        start_date=datetime(2022, 5, 23, 1),  # 2 är tiden på dygnet
        schedule_interval='@daily',
        template_searchpath=SQL_PATCH

) as dag:
    started = PythonOperator(
        task_id='started',
        python_callable=get_start
    )

    fanfictions = PythonOperator(
        task_id='Get_fanfictions_to_dw',
        python_callable=fanfiction_to_database,
        op_kwargs={'nbr': 2}

    )

    create_kpi_table = PostgresOperator(
        task_id='create_kpi_table',
        postgres_conn_id='postgres_localhost',
        sql=f"""
                   create table if not exists {TABLE_NAME}(
                   metric VARCHAR(50) NOT NULL, 
                   variable VARCHAR(50), 
                   timestamp TIMESTAMP NOT NULL, 
                   date DATE NOT NULL, 
                   value NUMERIC(19,4) NOT NULL
                   )
              """
    )

    METRIC_LIST = ['rating_count',
                   'category_count',
                   'word_day_average',
                   'word_rating_average',
                   'kudos_rating_sum',
                   'hits_rating_sum',
                   'language_count']
    metric_dag_list = []

    for metric in METRIC_LIST:
        metric_dag_list.append(metric_operator(
            metric=metric,
            dest_table=TABLE_NAME,
            source_conn_id='postgres_localhost',
            dest_conn_id='postgres_localhost'
        ))

    started >> fanfictions >> create_kpi_table >> metric_dag_list
