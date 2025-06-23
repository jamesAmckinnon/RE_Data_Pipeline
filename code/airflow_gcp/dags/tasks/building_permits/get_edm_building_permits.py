def get_edm_building_permits():   
    import json
    import pandas as pd
    from sqlalchemy import create_engine, Column, Integer, Float, String
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.schema import UniqueConstraint
    from sqlalchemy.dialects.postgresql import insert
    import requests
    from airflow.hooks.base import BaseHook
    from datetime import datetime
    from sodapy import Socrata
    from dotenv import load_dotenv
    import os
    from tqdm import tqdm
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    # Get database connection from Airflow connections
    def get_db_engine():
        conn = BaseHook.get_connection("supabase_db_TP_IPv4") 
        engine = create_engine(
            f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'
        )
        return engine

    # Create SQLAlchemy model for rental_rates
    Base = declarative_base()

    class BuildingPermit(Base):
        __tablename__ = 'building_permits'
        
        building_permit_id = Column(Integer, primary_key=True)
        uuid = Column(String)
        latitude = Column(Float)
        longitude = Column(Float)
        floor_area = Column(Float)
        address = Column(String)
        construction_value = Column(Float)
        num_units = Column(Integer)
        work_type = Column(String)
        building_type = Column(String)
        description = Column(String)
        permit_type = Column(String)
        permit_month = Column(Integer)
        permit_year = Column(Integer)
        issue_date = Column(String)
        neighbourhood = Column(String)
        zoning = Column(String)   

        __table_args__ = (
            UniqueConstraint('latitude', 'longitude', 'issue_date', name='unqc_lat_lon_issue_date'),
        )

        
        def __repr__(self):
            return f"<BuildingPermit(id={self.uuid}, work_type=${self.work_type}, description={self.description})>"


    load_dotenv()
    coe_username = os.getenv("COE_USERNAME")
    coe_password = os.getenv("COE_PASSWORD")

    client = Socrata("data.edmonton.ca",
                    "Op33anp9RGDX6ywsjFuVs8THM",
                    username=coe_username,
                    password=coe_password)

    # Calculate 4 months ago
    four_months_ago = datetime.now() - relativedelta(months=4)
    date_filter = four_months_ago.strftime('%Y-%m-%d')

    buildingPermits = [
        row for row in client.get_all("24uj-dj8v", 
        where=f"issue_date >= '{date_filter}'", 
        order="issue_date DESC")
    ]
    
    try:
        engine = get_db_engine()
        # Create tables if they don't exist
        Base.metadata.create_all(engine)
        
        # Create a session to interact with the database
        Session = sessionmaker(bind=engine)
        session = Session()

        db_building_permits = []
        for property in tqdm(buildingPermits, desc="Saving Building Permits"):

            db_building_permits.append({
                "uuid": property.get('uuid', ''),
                "latitude": property.get('latitude', 0.0),
                "longitude": property.get('longitude', 0.0),
                "floor_area": property.get('floor_area', 0.0),
                "address": property.get('address', ''),
                "construction_value": property.get('construction_value', 0.0),
                "num_units": property.get('units_added', 0),
                "work_type": property.get('work_type', ''),
                "building_type": property.get('building_type', ''),
                "description": property.get('job_description', ''),
                "permit_type": property.get('job_category', ''),
                "permit_month": property.get('month_number', 0),
                "permit_year": property.get('year', 0),
                "issue_date": property.get('issue_date', ''),    
                "neighbourhood": property.get('neighbourhood', ''),    
                "zoning": property.get('zoning', '')    
            })

        stmt = insert(BuildingPermit).values(db_building_permits)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=['latitude', 'longitude', 'issue_date']
        ).returning(BuildingPermit)

        result = session.execute(stmt)
        inserted_count = result.rowcount
        inserted_records = result.fetchall()
        session.commit()

        print(f"First 5 records added to DB (if there were 5 to add):")
        for i, record in enumerate(inserted_records[:5]):
            print(f"Record {i+1}: {record}")

        print(f"Successfully saved {inserted_count} building permit records to the database")
    
    except Exception as e:
        if 'session' in locals():
            session.rollback()
        print(f"Error saving to database: {e}")
    
    finally:
        if 'session' in locals():
            session.close()
    
    return