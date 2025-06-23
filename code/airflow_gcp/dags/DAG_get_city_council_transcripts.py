from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.operators.bash import BashOperator 

from tasks.council_transcripts.get_edm_council_transcripts import get_edm_council_transcripts


default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}   


with DAG(
    default_args=default_args,
    dag_id='get_council_transcripts',
    description='Gets the transcript data for a city',
    start_date=datetime(2025, 4, 4),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    edmonton_transcripts = PythonOperator(
        task_id='get_edm_council_transcripts',
        python_callable=get_edm_council_transcripts
    )

    # zoning_data = PythonOperator(
    #     task_id='get_zoning_data',
    #     python_callable=get_zoning_data
    # )

    transcript_tasks_complete = BashOperator(
        task_id='transcript_tasks_complete',
        bash_command='echo All transcript tasks complete.',
    )

edmonton_transcripts >> transcript_tasks_complete