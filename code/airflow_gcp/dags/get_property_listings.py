from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup 

from tasks.listings.get_AV_listings import get_AV_listings
from tasks.listings.get_omada_listings import get_omada_listings
from tasks.listings.get_royal_park_listings import get_royal_park_listings

from tasks.additional_listing_info.get_brochure_info import get_brochure_info
from tasks.additional_listing_info.get_osm_data import get_osm_data
from tasks.additional_listing_info.get_zoning_data import get_zoning_data

from tasks.combine_broker_listings import combine_broker_listings


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

with DAG(
    default_args=default_args,
    dag_id='get_property_listings_dag',
    description='Gets the property listings from brokerage websites',
    start_date=datetime(2025, 4, 4),
    schedule_interval='@weekly',
) as dag:
    
    # Get property listings from brokerage websites
    AV_listings = PythonOperator(
        task_id='get_AV_listings',
        python_callable=get_AV_listings,
        op_kwargs={
            'gcs_bucket': gcs_bucket,
            'gcs_path': f"{gcs_separated_property_listings}/av_listings.json"
        }
    )

    omada_listings = PythonOperator(
        task_id='get_omada_listings',
        python_callable=get_omada_listings,
        op_kwargs={
            'gcs_bucket': gcs_bucket,
            'gcs_path': f"{gcs_separated_property_listings}/omada_listings.json"
        }
    )

    royal_park_listings = PythonOperator(
        task_id='get_royal_park_listings',
        python_callable=get_royal_park_listings,
        op_kwargs={
            'gcs_bucket': gcs_bucket,
            'gcs_path': f"{gcs_separated_property_listings}/royal_park_listings.json"
        }
    )

    # Runs when all listings tasks have completed
    listing_tasks_complete = BashOperator(
        task_id='all_listing_tasks_complete',
        bash_command='echo All listing tasks complete.',
    )

    # Get additional information for listings
    brochure_information = PythonOperator(
        task_id='get_brochure_info',
        python_callable=get_brochure_info,
        op_kwargs={
            'gcs_bucket': gcs_bucket,
            'input_path': gcs_separated_property_listings,
            'output_path': f"{gcs_separated_listing_info}/brochure_info.json"
        }
    )

    osm_data = PythonOperator(
        task_id='get_osm_data',
        python_callable=get_osm_data,
        op_kwargs={
            'gcs_bucket': gcs_bucket,
            'input_path': gcs_separated_property_listings,
            'output_path': f"{gcs_separated_listing_info}/osm_data.json"
        }
    )

    zoning_data = PythonOperator(
        task_id='get_zoning_data',
        python_callable=get_zoning_data,
        op_kwargs={
            'gcs_bucket': gcs_bucket,
            'input_path': gcs_separated_property_listings,
            'output_path': f"{gcs_separated_listing_info}/zoning_data.json"
        }
    )


    # Combine all listings and add to the DB
    combine_and_format_listings = PythonOperator(
        task_id='combine_broker_listings',
        python_callable=combine_broker_listings,
        op_kwargs={
            'gcs_bucket': gcs_bucket,
            'properties_input_path': gcs_separated_property_listings,
            'properties_info_input_path': gcs_separated_listing_info,
            'output_path': f"{gcs_all_listings_combined}/combined_listings.json"
        }
    )


[AV_listings, omada_listings, royal_park_listings] >> listing_tasks_complete \
>> [brochure_information, osm_data, zoning_data] >> combine_and_format_listings
