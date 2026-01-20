from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 

import sys 
import os


def start_dag():
    print("start dag")


default_args = {
    "owner": "Ahmed_refat",
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}

with DAG(
    dag_id="nyc_traffic_data_pipline",
    start_date=datetime(2026, 1, 16),
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    start_project=PythonOperator(
        task_id="start_dag",
        python_callable=start_dag

    )

    #task 1 (exetract data and store it in data_lake(s3))
    extract_data = BashOperator(
    task_id="extract_data",
    bash_command="python /opt/airflow/scripts/extract_traffic.py"
    )

    #==============================
    # task 2 (processing first source )
    proceesing_one = BashOperator(
    task_id="proceesing_one",
    bash_command="python /opt/airflow/scripts/processing1_traffic.py"
    )

    #==============================
    # task 3 (processing second source )
    processing_two = BashOperator(
    task_id="processing_two",
    bash_command="python /opt/airflow/scripts/processing2_traffic.py"
    )
    
    #==============================
    # task 4 (processing third source )
    processing_three = BashOperator(
    task_id="processing_three",
    bash_command="python /opt/airflow/scripts/processing3_traffic.py"
    )

    #==============================
    # task 5 (create dimentions )
    create_dimentions = BashOperator(
    task_id="create_dimentions",
    bash_command="python /opt/airflow/scripts/create_dims_traffic.py"
    )

    #================================
    # tsak 6 (create facts )
    create_facts = BashOperator(
    task_id="create_facts",
    bash_command="python /opt/airflow/scripts/create_facts_traffic.py"
    )

    #================================
    # tsak 7 (load data )
    load_data = BashOperator(
    task_id="load_data",
    bash_command="python /opt/airflow/scripts/load_traffic.py"
    )

    extract_data >> proceesing_one >>processing_two >> processing_three >> create_dimentions >>create_facts >> load_data
