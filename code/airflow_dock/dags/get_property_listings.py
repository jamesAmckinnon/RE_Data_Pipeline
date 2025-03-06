from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore

from tasks.listings.get_AV_listings import get_AV_listings
from tasks.listings.get_omada_listings import get_omada_listings
from tasks.listings.get_royal_park_listings import get_royal_park_listings

from tasks.additional_listing_info.get_brochure_info import get_brochure_info
from tasks.additional_listing_info.get_osm_data import get_osm_data
from tasks.additional_listing_info.get_zoning_data import get_zoning_data

from tasks.combine_broker_listings import combine_broker_listings


default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}   

with DAG(
    default_args=default_args,
    dag_id='get_property_listings_dag',
    description='Gets the property listings from brokerage websites',
    start_date=datetime(2025, 2, 21),
    schedule_interval='@daily',
) as dag:
    
    # Get property listings from brokerage websites
    AV_listings = PythonOperator(
        task_id='get_AV_listings',
        python_callable=get_AV_listings
    )

    omada_listings = PythonOperator(
        task_id='get_omada_listings',
        python_callable=get_omada_listings
    )

    royal_park_listings = PythonOperator(
        task_id='get_royal_park_listings',
        python_callable=get_royal_park_listings
    )


    # Runs when all listings tasks have completed
    listing_tasks_complete = BashOperator(
        task_id='all_listing_tasks_complete',
        bash_command='echo All listing tasks complete.',
    )


    # Get additional information for listings
    brochure_information = PythonOperator(
        task_id='get_brochure_info',
        python_callable=get_brochure_info
    )

    osm_data = PythonOperator(
        task_id='get_osm_data',
        python_callable=get_osm_data
    )

    zoning_data = PythonOperator(
        task_id='get_zoning_data',
        python_callable=get_zoning_data
    )


    # Combine all listings and format
    # combine_and_format_listings = PythonOperator(
    #     task_id='combine_broker_listings',
    #     python_callable=combine_broker_listings
    # )


[AV_listings, omada_listings, royal_park_listings] >> listing_tasks_complete \
>> [brochure_information, osm_data, zoning_data] # >> combine_and_format_listings
