from airflow import DAG
from datetime import datetime, timedelta
from airflow.sensors.http_sensor import HttpSensor
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.slack_operator import SlackAPIPostOperator

import json
import csv
import requests

default_args = {
    "owner" : 'test',
    "start_date" : datetime(2021, 1, 1),
    "depends_on_past" : False,
    "email_on_failure" : False,
    "email" : "youremail@host.com",
    "retries" : 1,
    "retry_delay" : timedelta(minutes=5)
}

with DAG(dag_id = "test",
schedule_interval = "@hourly", 
default_args = default_args, 
catchup = True) as dag:
    collect_data = HttpSensor(
        task_id = "collect_data",
        method = "GET",
        http_conn_id = "data_api",
        endpoint = "latest",
        response_check = lambda response: "rates" in response.text ,
        poke_interval = 5,
        timeout = 20
    )