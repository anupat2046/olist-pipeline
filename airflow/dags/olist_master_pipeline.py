"""
Olist Master Pipeline DAG
=========================

DAG หลักสำหรับรัน Pipeline ทั้งหมด
ตั้งเวลารันทุกวันตี 2 เพื่อให้ข้อมูลพร้อมใช้ก่อนเริ่มงาน

Data Flow:
    CSV -> Spark -> Parquet -> GCS -> BigQuery (Silver)
                                          |
                                    dbt (Gold Layer)
                                          |
                               fct_sales_performance
                               dim_customers_metrics
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Path ของโปรเจกต์ใน Docker container
PROJECT_PATH = "/opt/airflow/project"

# ตั้งค่าพื้นฐานสำหรับทุก task
default_args = {
    'owner': 'anupat',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def log_pipeline_start(**context):
    """บันทึกเวลาเริ่มต้น pipeline ลง BigQuery"""
    import sys
    sys.path.insert(0, PROJECT_PATH)
    
    from utils.gcp_utils import JobLogger
    
    run_id = context['run_id']
    logger = JobLogger(config_path=f"{PROJECT_PATH}/config/settings.yaml")
    
    record = logger.log_start(job_id=run_id, step_name="pipeline_start")
    logger.log_end(record, status="success", metadata={"dag": "olist_master_pipeline"})
    
    return run_id


def log_pipeline_end(**context):
    """บันทึกเวลาจบ pipeline ลง BigQuery"""
    import sys
    sys.path.insert(0, PROJECT_PATH)
    
    from utils.gcp_utils import JobLogger
    
    run_id = context['run_id']
    logger = JobLogger(config_path=f"{PROJECT_PATH}/config/settings.yaml")
    
    record = logger.log_start(job_id=run_id, step_name="pipeline_complete")
    logger.log_end(record, status="success", metadata={"dag": "olist_master_pipeline"})


with DAG(
    dag_id='olist_master_pipeline_v1',
    default_args=default_args,
    description='Pipeline หลัก: Silver to Gold Layer',
    schedule_interval='0 2 * * *',  # รันทุกวันตี 2
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['olist', 'data_engineering', 'gold_layer'],
    doc_md=__doc__,
) as dag:
    
    # บันทึกเวลาเริ่มต้น
    start_logging = PythonOperator(
        task_id='log_pipeline_start',
        python_callable=log_pipeline_start,
        provide_context=True,
    )
    
    # Silver Layer: Spark -> GCS -> BigQuery
    silver_layer = BashOperator(
        task_id='process_silver_layer',
        bash_command=f'''
            cd {PROJECT_PATH} && \
            python spark/build_silver.py && \
            python -c "from utils.gcp_utils import GCPUtils; GCPUtils().upload_folder('data/silver', 'silver')" && \
            python ingest/bq_loader.py
        ''',
        env={
            'GOOGLE_APPLICATION_CREDENTIALS': f'{PROJECT_PATH}/.google/service-account.json',
        },
    )
    
    # Gold Layer: dbt run + test
    # ลบ cache ก่อนเพื่อกัน error
    gold_layer = BashOperator(
        task_id='process_gold_layer',
        bash_command=f'''
            cd {PROJECT_PATH}/dbt_olist && \
            rm -rf dbt_packages package-lock.yml && \
            dbt deps && \
            dbt run && \
            dbt test
        ''',
    )
    
    # สร้าง documentation
    generate_docs = BashOperator(
        task_id='generate_dbt_docs',
        bash_command=f'''
            cd {PROJECT_PATH}/dbt_olist && \
            dbt docs generate
        ''',
    )
    
    # บันทึกเวลาจบ
    end_logging = PythonOperator(
        task_id='log_pipeline_end',
        python_callable=log_pipeline_end,
        provide_context=True,
    )
    
    # ลำดับการทำงาน
    start_logging >> silver_layer >> gold_layer >> generate_docs >> end_logging
