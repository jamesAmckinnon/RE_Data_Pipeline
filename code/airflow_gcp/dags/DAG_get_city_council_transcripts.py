from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.operators.bash import BashOperator 

from tasks.council_transcripts.get_edm_council_transcripts import get_edm_council_transcripts
from tasks.council_transcripts.transcripts_to_vector_db import transcripts_to_vector_db
# from tasks.council_transcripts.summaries_old import transcript_vectors_to_summaries
from tasks.council_transcripts.transcript_summaries import transcript_vectors_to_summaries


default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}   

gcs_bucket = "council-transcripts"
gcs_edm_council_transcripts = "edm_council_transcripts"

with DAG(
    default_args=default_args,
    dag_id='get_council_transcripts',
    description='Gets the transcript data for a city',
    start_date=datetime(2025, 4, 4),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    # edmonton_transcripts = PythonOperator(
    #     task_id='get_edm_council_transcripts',
    #     python_callable=get_edm_council_transcripts,
    #     op_kwargs={
    #         'gcs_bucket': gcs_bucket,
    #         'gcs_output_path': f"{gcs_edm_council_transcripts}/edm_council_transcripts.json"
    #     }
    # )

    # update_vector_db = PythonOperator(
    #     task_id='transcripts_to_vector_db',
    #     python_callable=transcripts_to_vector_db,
    #     op_kwargs={
    #         'gcs_bucket': gcs_bucket,
    #         'gcs_input_path': gcs_edm_council_transcripts
    #     }
    # )

    summarize_meetings = PythonOperator(
        task_id='transcript_vectors_to_summaries',
        python_callable=transcript_vectors_to_summaries,
        op_kwargs={
            'gcs_bucket': gcs_bucket,
            'gcs_input_path': gcs_edm_council_transcripts
        }
    )

# edmonton_transcripts >> update_vector_db >> summarize_meetings
summarize_meetings