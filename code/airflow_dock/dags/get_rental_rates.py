from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator # type: ignore

from tasks.rental_rates.get_liv_data import get_liv_data
from tasks.rental_rates.combine_and_format import combine_and_format
from tasks.rental_rates.aggregate_rental_rates import aggregate_rental_rates


# In the future will change dynamically for whatever city is currently being processed
lat, lon = 53.53182005, -113.4956931182721

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}   

with DAG(
    default_args=default_args,
    dag_id='get_rental_rates_dag',
    description='Gets the rental rates from rental listing websites',
    start_date=datetime(2025, 2, 21),
    schedule_interval='@daily',
) as dag:
    
    # Get rental rates from rental listing sites
    liv_data = PythonOperator(
        task_id='get_liv_data',
        python_callable=get_liv_data
    )

    # rent_faster = PythonOperator(
    #     task_id='get_rent_faster_data',
    #     python_callable=get_rent_faster_data
    # )

    # Runs when all rental rate tasks have completed
    rental_rate_tasks_complete = BashOperator(
        task_id='all_rental_rate_tasks_complete',
        bash_command='echo All rental rate tasks complete.',
    )

    combine_and_format = PythonOperator(
        task_id='combine_and_format',
        python_callable=combine_and_format
    )

    aggregate_rental_rates = PythonOperator(
        task_id='aggregate_rental_rates',
        python_callable=aggregate_rental_rates,
            op_kwargs={
            "center_lat": lat, 
            "center_lon": lon, 
            "grid_size": 32000,
            "cell_size": 500
        }
    )

[liv_data] >> rental_rate_tasks_complete >> combine_and_format >> aggregate_rental_rates