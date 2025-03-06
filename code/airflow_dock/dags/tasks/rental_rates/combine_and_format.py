# Combine all rental rate files into one
def combine_and_format():   
    from pathlib import Path
    import json
    import pandas as pd
    from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Table, MetaData
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    import datetime
    from airflow.hooks.base import BaseHook

    input_bucket = Path("/opt/airflow/data/separated_rental_rates")
    output_bucket = Path("/opt/airflow/data/combined_rental_rates")

    # Get database connection from Airflow connections
    def get_db_engine():
        conn = BaseHook.get_connection("postgres_localhost")
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
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
        created_at = Column(DateTime, default=datetime.datetime.now(datetime.timezone.utc))
        
        def __repr__(self):
            return f"<RentalRate(id={self.uuid}, rental_rate=${self.rental_rate}, size={self.size})>"



    # Get all rental rate files from input bucket
    rental_rate_files = list(input_bucket.glob("*.json"))

    all_rental_rates = []

    for file in rental_rate_files:
        with open(file, "r") as f:
            rental_rates = json.load(f)
            all_rental_rates.extend(rental_rates)

    # Save combined output to file
    with open(output_bucket / "combined_rental_rates.json", "w") as f:
        json.dump(all_rental_rates, f)



    ################################################
    ########  Save Rental Rates to the DB  #########
    ################################################
    
    # Create database engine
    engine = get_db_engine()
    # Create tables if they don't exist
    Base.metadata.create_all(engine)
    
    # Create a session to interact with the database
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Convert the data to a list of RentalRate objects
        db_rental_rates = []
        for rate in all_rental_rates:
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
        session.rollback()
        print(f"Error saving to database: {e}")
    
    finally:
        session.close()