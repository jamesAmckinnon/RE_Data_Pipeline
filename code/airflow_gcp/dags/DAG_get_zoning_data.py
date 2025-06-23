from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator 

from tasks.zoning.get_edm_rezoning_data import get_edm_rezoning_data


default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}   


with DAG(
    default_args=default_args,
    dag_id='get_zoning_data',
    description='Gets the zoning data for a city',
    start_date=datetime(2025, 4, 4),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    rezoning_data = PythonOperator(
        task_id='get_edm_rezoning_data',
        python_callable=get_edm_rezoning_data
    )

    # zoning_data = PythonOperator(
    #     task_id='get_zoning_data',
    #     python_callable=get_zoning_data
    # )

    zoning_tasks_complete = BashOperator(
        task_id='zoning_tasks_complete',
        bash_command='echo All zoning tasks complete.',
    )

rezoning_data >> zoning_tasks_complete