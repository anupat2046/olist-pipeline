"""
Olist Pipeline DAG - Gold Layer Only
=====================================
รัน dbt Gold Layer (หลังจากรัน Silver Layer ในเครื่องแล้ว)

Schedule: Daily at 02:00 AM
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

PROJECT_PATH = "/opt/airflow/project"

default_args = {
    'owner': 'anupat',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='olist_gold_layer',
    default_args=default_args,
    description='dbt Gold Layer Pipeline',
    schedule_interval='0 2 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['olist', 'dbt', 'gold_layer'],
) as dag:
    
    # Task 1: Run dbt models
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {PROJECT_PATH}/dbt_olist && dbt deps && dbt run',
    )
    
    # Task 2: Run dbt tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {PROJECT_PATH}/dbt_olist && dbt test',
    )
    
    # Task 3: Generate docs
    dbt_docs = BashOperator(
        task_id='dbt_docs',
        bash_command=f'cd {PROJECT_PATH}/dbt_olist && dbt docs generate',
    )
    
    dbt_run >> dbt_test >> dbt_docs
