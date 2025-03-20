from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator # type: ignore

from tasks.financial_reports.REIT_metrics import get_REIT_report_data


default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}   

with DAG(
    default_args=default_args,
    dag_id='get_financial_report_data_dag',
    description='Gets financial metrics from financial reports',
    start_date=datetime(2025, 3, 24),
    schedule_interval='@daily',
) as dag:
    
    # Get rental rates from rental listing sites
    REIT_data = PythonOperator(
        task_id='get_RIET_data',
        python_callable=get_REIT_report_data
    )

REIT_data