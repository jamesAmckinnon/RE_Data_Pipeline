from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator 

from tasks.financial_reports.REIT_metrics import get_REIT_report_data


gcs_bucket = "cre-financial-reports"
gcs_reports_pdfs = "reports_pdfs"

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}   

with DAG(
    default_args=default_args,
    dag_id='get_financial_report_data_dag',
    description='Gets financial metrics from financial reports',
    start_date=datetime(2025, 4, 7),
    schedule_interval=None,
    catchup=False
) as dag:
    
    # Download and parse the REIT reports
    REIT_data = PythonOperator(
        task_id='get_RIET_data',
        python_callable=get_REIT_report_data,
        op_kwargs={
            'gcs_bucket': gcs_bucket,
            'gcs_input_path': gcs_reports_pdfs
        }
    )

REIT_data