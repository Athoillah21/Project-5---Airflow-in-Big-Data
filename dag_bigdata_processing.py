#!python airflow

from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

default_args = {
    'owner': 'athoillah',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)}

with DAG(
    default_args=default_args,
    dag_id='Big_Data_Processing',
    description='ETL Using BashOperator and PostgresOperator',
    start_date=datetime(2022, 11, 23),
    schedule_interval='59 23 * * *'
    ) as dag:

    t1 = DummyOperator(
        task_id='Start')

    t2 = BashOperator(
        task_id='Dump_Data',
        bash_command='python3 /home/athoillah/Downloads/Data Engineering_DigitalSkola/Project/Project-5/dump.py',
        dag=dag)

    t3 = BashOperator(
        task_id='ETL_Mapreduce',
        bash_command='python3 /home/athoillah/Downloads/Data Engineering_DigitalSkola/Project/Project-5/mapreduce_etl.py',
        dag=dag)

    t4 = DummyOperator(
        task_id='Stop')

    t1 >> t2 >> t3 >> t4