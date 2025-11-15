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
    import traceback
    from pathlib import Path
    import time
    
    # Configs stored in Google VM instance
    config_dir = Path("/home/jamesamckinnon1/air_env/configs")
    
    # Get database connection from Airflow connections
    def get_db_engine():
        conn = BaseHook.get_connection("supabase_db_TP_IPv4") 
        engine = create_engine(
            f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'
        )
        return engine
    
    def safe_cast(val, to_type, default):
        try:
            if val is None or pd.isna(val) or val in ["NaN", "nan"]:
                return default
            if to_type is str:
                return str(val)
            return to_type(val)
        except (ValueError, TypeError):
            return default
    
    def extract_day_from_issue_date(issue_date_str):
        try:
            return datetime.strptime(issue_date_str, "%Y-%m-%dT%H:%M:%S.%f").day
        except Exception:
            return 0
    
    def geocode_address(address, api_key):
        """
        Use Google Geocoding API to get lat/lon from address.
        Returns tuple (latitude, longitude) or (None, None) if geocoding fails.
        """
        if not address or address == 'Unknown':
            return None, None
        
        # Add "Edmonton, Alberta, Canada" to improve geocoding accuracy
        full_address = f"{address}, Edmonton, Alberta, Canada"
        
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            'address': full_address,
            'key': api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data['status'] == 'OK' and len(data['results']) > 0:
                location = data['results'][0]['geometry']['location']
                return location['lat'], location['lng']
            else:
                print(f"Geocoding failed for address: {address} - Status: {data.get('status')}")
                return None, None
                
        except Exception as e:
            print(f"Error geocoding address {address}: {str(e)}")
            return None, None
    
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
        permit_day = Column(Integer)
        issue_date = Column(String)
        neighbourhood = Column(String)
        zoning = Column(String)   
        
        __table_args__ = (
            UniqueConstraint('latitude', 'longitude', 'issue_date', name='unqc_lat_lon_issue_date'),
        )
        
        def __repr__(self):
            return f"<BuildingPermit(id={self.uuid}, work_type=${self.work_type}, description={self.description})>"
    
    # Load environment variables
    load_dotenv(dotenv_path=config_dir / ".env")
    coe_username = os.getenv("COE_USERNAME")
    coe_password = os.getenv("COE_PASSWORD")
    google_api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    
    if not google_api_key:
        raise ValueError("GOOGLE_MAPS_API_KEY not found in .env file")
    
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
        
        num_permits = len(buildingPermits)
        db_building_permits = []
        geocoded_count = 0
        skipped_count = 0
        
        for i, property in enumerate(buildingPermits):
            if i % 100 == 0:
                print(f"Processing permit {i} of {num_permits}")
            
            issue_date = property.get('issue_date', '')
            address = safe_cast(property.get('address'), str, 'Unknown')
            
            # Get coordinates from API data or default to 0.0
            latitude = safe_cast(property.get('latitude'), float, 0.0)
            longitude = safe_cast(property.get('longitude'), float, 0.0)
            
            # If coordinates are missing (0.0), try to geocode the address
            if (latitude == 0.0 or longitude == 0.0) and address != 'Unknown':
                print(f"Geocoding address: {address}")
                lat, lon = geocode_address(address, google_api_key)
                
                if lat is not None and lon is not None:
                    latitude = lat
                    longitude = lon
                    geocoded_count += 1
                    print(f"Successfully geocoded: {address} -> ({lat}, {lon})")
                else:
                    print(f"Failed to geocode address: {address} - Skipping record")
                    skipped_count += 1
                    continue  # Skip this record entirely
                
                # Add a small delay to respect API rate limits
                time.sleep(0.02)
            
            # Skip records that still have 0.0 coordinates
            if latitude == 0.0 or longitude == 0.0:
                print(f"Skipping record with missing coordinates: {address}")
                skipped_count += 1
                continue
            
            db_building_permits.append({
                "uuid": safe_cast(property.get('uuid'), str, 'Unknown'),
                "latitude": latitude,
                "longitude": longitude,
                "floor_area": safe_cast(property.get('floor_area'), float, 0.0),
                "address": address,
                "construction_value": safe_cast(property.get('construction_value'), float, 0.0),
                "num_units": safe_cast(property.get('units_added'), int, 0),
                "work_type": safe_cast(property.get('work_type'), str, 'Unknown'),
                "building_type": safe_cast(property.get('building_type'), str, 'Unknown'),
                "description": safe_cast(property.get('job_description'), str, 'Unknown'),
                "permit_type": safe_cast(property.get('job_category'), str, 'Unknown'),
                "permit_month": safe_cast(property.get('month_number'), int, 0),
                "permit_year": safe_cast(property.get('year'), int, 0),
                "permit_day": extract_day_from_issue_date(issue_date),
                "issue_date": safe_cast(issue_date, str, 'Unknown'),
                "neighbourhood": safe_cast(property.get('neighbourhood'), str, 'Unknown'),
                "zoning": safe_cast(property.get('zoning'), str, 'Unknown'),
            })
        
        print(f"\nProcessing summary:")
        print(f"Total permits processed: {num_permits}")
        print(f"Successfully geocoded: {geocoded_count}")
        print(f"Skipped (no coordinates): {skipped_count}")
        print(f"Ready to insert: {len(db_building_permits)}")
        
        CHUNK_SIZE = 500
        inserted_count = 0
        
        for i in range(0, len(db_building_permits), CHUNK_SIZE):
            print(f"Saving records {i}-{min(i + CHUNK_SIZE, len(db_building_permits))} of {len(db_building_permits)} to the database")
            chunk = db_building_permits[i:i + CHUNK_SIZE]
            
            stmt = insert(BuildingPermit).values(chunk)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=['latitude', 'longitude', 'issue_date']
            )
            
            result = session.execute(stmt)
            inserted_count += result.rowcount
        
        session.commit()
        print(f"\nSuccessfully saved {inserted_count} building permit records to the database")
    
    except Exception as e:
        if 'session' in locals():
            session.rollback()
        print(f"Error saving to database: {traceback.format_exc()}")
    
    finally:
        if 'session' in locals():
            session.close()
    
    return