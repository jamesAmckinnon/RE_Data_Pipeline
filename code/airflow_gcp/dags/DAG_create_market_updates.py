from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup 

# from tasks.listings.get_transcript_data import get_transcript_data

gcs_bucket = "cre-property-listings"
gcs_separated_property_listings = "separated_property_listings"
gcs_separated_brochure_data = "separated_brochure_info"
gcs_separated_listing_info = "separated_listing_info"
gcs_all_listings_combined = "all_listings_combined"

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}   

# with DAG(
#     default_args=default_args,
#     dag_id='get_transcript_data_dag',
#     description='Creates the market update using data from the Supabase and Pinecone DBs',
#     start_date=datetime(2025, 4, 4),
#     schedule_interval='@weekly',
#     catchup=False
# ) as dag:
    
    # # Get property listings from brokerage websites
    # edm_transcripts = PythonOperator(
    #     task_id='get_AV_listings',
    #     python_callable=get_transcript_data,
    #     op_kwargs={
    #         'gcs_bucket': gcs_bucket,
    #         'gcs_path': f"{gcs_separated_property_listings}/av_listings.json"
    #     }
    # )