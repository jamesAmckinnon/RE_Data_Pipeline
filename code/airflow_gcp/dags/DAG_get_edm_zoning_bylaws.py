from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.operators.bash import BashOperator 

from tasks.zoning.get_edm_zoning_bylaw_data import get_edm_zoning_bylaw_data


default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}   


with DAG(
    default_args=default_args,
    dag_id='get_edm_zoning_bylaw_data',
    description='Gets zoning bylaw data for Edmonton',
    start_date=datetime(2025, 4, 4),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    edm_zoning_bylaw_data = PythonOperator(
        task_id='get_edm_zoning_bylaw_data',
        python_callable=get_edm_zoning_bylaw_data
    )

edm_zoning_bylaw_data