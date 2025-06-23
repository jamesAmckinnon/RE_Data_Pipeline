from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.operators.bash import BashOperator 

from tasks.building_permits.get_edm_building_permits import get_edm_building_permits


default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}   


with DAG(
    default_args=default_args,
    dag_id='get_building_permits',
    description='Gets the building permit data',
    start_date=datetime(2025, 4, 4),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    edm_building_permits = PythonOperator(
        task_id='get_edm_building_permits',
        python_callable=get_edm_building_permits
    )

edm_building_permits