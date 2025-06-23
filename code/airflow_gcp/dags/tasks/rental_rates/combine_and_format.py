# Combine all rental rate files into one
def combine_and_format(gcs_bucket, input_path, output_path):   
    import json
    import pandas as pd
    from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    import datetime
    from airflow.hooks.base import BaseHook
    from google.cloud import storage

    # Get database connection from Airflow connections
    def get_db_engine():
        conn = BaseHook.get_connection("supabase_db_TP_IPv4") 
        engine = create_engine(
            f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'
        )
        return engine

    # Create SQLAlchemy model for rental_rates
    Base = declarative_base()

    class RentalRate(Base):
        __tablename__ = 'rent_listings'
        
        rent_unit_id = Column(Integer, primary_key=True)
        uuid = Column(String)
        building_name = Column(String)
        rental_rate = Column(String)
        building_type = Column(String)
        address = Column(String)
        city = Column(String)
        province = Column(String)
        latitude = Column(Float)
        longitude = Column(Float)
        bedrooms = Column(Integer)
        bathrooms = Column(Integer)
        size = Column(Integer)
        
        def __repr__(self):
            return f"<RentalRate(id={self.uuid}, rental_rate=${self.rental_rate}, size={self.size})>"


    # Initialize GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)
    
    # List all blobs in the input path
    blobs = list(bucket.list_blobs(prefix=input_path))
    
    # Filter for only JSON files
    json_blobs = [blob for blob in blobs if blob.name.endswith('.json')]
    
    print(f"Found {len(json_blobs)} JSON files in {gcs_bucket}/{input_path}")

    all_rental_rates = []
    for blob in json_blobs:
        content = blob.download_as_string()
        rental_rates = json.loads(content)
        all_rental_rates.extend(rental_rates)
    
    print(f"Combined {len(all_rental_rates)} rental rates from all files")

    # Save combined output to GCS
    output_blob = bucket.blob(output_path)
    output_blob.upload_from_string(
        json.dumps(all_rental_rates), 
        content_type='application/json'
    )
    
    print(f"Saved combined data to gs://{gcs_bucket}/{output_path}")

    ################################################
    ########  Save Rental Rates to the DB  #########
    ################################################
    
    try:

        # Create database engine
        engine = get_db_engine()
        # Create tables if they don't exist
        Base.metadata.create_all(engine)
        
        # Create a session to interact with the database
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Convert the data to a list of RentalRate objects
        db_rental_rates = []
        for rate in all_rental_rates:
            exists = session.query(RentalRate).filter_by(
                building_name=rate.get('building_name', ''),
                rental_rate=rate.get('rental_rate', ''),
                building_type=rate.get('building_type', ''),
                address=rate.get('address', ''),
                city=rate.get('city', ''),
                province=rate.get('province', ''),
                latitude=rate.get('latitude', 0.0),
                longitude=rate.get('longitude', 0.0),
                bedrooms=rate.get('bedrooms', 0),
                bathrooms=rate.get('bathrooms', 0.0),
                size=rate.get('size', 'unknown'),
            ).first()

            if not exists:
                db_rental_rates.append(
                    RentalRate(
                        uuid=rate.get('uuid', ''),
                        building_name=rate.get('building_name', ''),
                        rental_rate=rate.get('rental_rate', ''),
                        building_type=rate.get('building_type', ''),
                        address=rate.get('address', ''),
                        city=rate.get('city', ''),
                        province=rate.get('province', ''),
                        latitude=rate.get('latitude', 0.0),
                        longitude=rate.get('longitude', 0.0),
                        bedrooms=rate.get('bedrooms', 0),
                        bathrooms=rate.get('bathrooms', 0.0),
                        size=rate.get('size', 'unknown'),
                    )
                )
        
        # Add all rental rates to the database
        session.add_all(db_rental_rates)
        session.commit()
        print(f"Successfully saved {len(db_rental_rates)} rental rates to the database")
    
    except Exception as e:
        if 'session' in locals():
            session.rollback()
        print(f"Error saving to database: {e}")
    
    finally:
        if 'session' in locals():
            session.close()
    
    return f"gs://{gcs_bucket}/{output_path}"