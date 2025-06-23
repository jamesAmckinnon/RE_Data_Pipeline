def get_edm_rezoning_data():   
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

    # Get database connection from Airflow connections
    def get_db_engine():
        conn = BaseHook.get_connection("supabase_db_TP_IPv4") 
        engine = create_engine(
            f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'
        )
        return engine

    # Create SQLAlchemy model for rental_rates
    Base = declarative_base()

    class RezonedProperty(Base):
        __tablename__ = 'rezoning_data'
        
        rezoning_id = Column(Integer, primary_key=True)
        uuid = Column(String)
        file_number = Column(String)
        created_date = Column(String)
        application_type = Column(String)
        address = Column(String)
        city = Column(String)
        province = Column(String)
        latitude = Column(Float)
        longitude = Column(Float)
        zone_change = Column(String)
        status = Column(String)
        council_public_hearing_date = Column(String)
        council_approval_date = Column(String)
        planner_contact = Column(String)

        __table_args__ = (
            UniqueConstraint('latitude', 'longitude', 'created_date', name='uq_lat_lon_created'),
        )

        
        def __repr__(self):
            return f"<RezonedProperty(id={self.uuid}, zone_change=${self.zone_change}, status={self.status})>"
        

    def get_edmonton_data(limit=10000, start_date="2005-01-01", end_date="now"):
        if end_date is 'now':
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        where_clause = f"status IN ('In Review') OR (status IN ('Approved','Refused')" \
                       f" AND council_approval_date between '{start_date}' and '{end_date}')"
        
        url = f"https://www.edmonton.ca/open-data/8847?$limit={limit}&$where={where_clause}"\
              f"&_={int(datetime.now().timestamp() * 1000)}"
        
        response = requests.get(url)
        return response
    
   
    limit=10000
    start_date = "2025-06-18"
    end_date = "now"
    rezonedProperties = None

    response = get_edmonton_data(limit=limit, start_date=start_date, end_date=end_date)
    if response.status_code == 200:
        data = response.json()
        rezonedProperties=data

    engine = get_db_engine()
    # Create tables if they don't exist
    Base.metadata.create_all(engine)
    
    # Create a session to interact with the database
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        db_rezoned_properties = []
        for property in rezonedProperties:
            if property.get("application_type") != "Rezoning":
                continue

            db_rezoned_properties.append({
                "uuid": property.get('uuid', ''),
                "file_number": property.get('file_number', ''),
                "created_date": property.get('created_date', ''),
                "application_type": property.get('application_type', ''),
                "address": property.get('address', ''),
                "city": property.get('city', ''),
                "province": property.get('province', ''),
                "latitude": property.get('latitude', 0.0),
                "longitude": property.get('longitude', 0.0),
                "zone_change": property.get('zone_change', ''),
                "status": property.get('status', ''),
                "council_public_hearing_date": property.get('council_public_hearing_date', ''),
                "council_approval_date": property.get('council_approval_date', ''),
                "planner_contact": property.get('planner_contact', '')
            }
        )
        

        stmt = insert(RezonedProperty).values(db_rezoned_properties)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=['latitude', 'longitude', 'created_date']
        )

        result = session.execute(stmt)
        inserted_count = result.rowcount
        session.commit()

        print(f"Successfully saved {inserted_count} rezoning records to the database")
    
    except Exception as e:
        if 'session' in locals():
            session.rollback()
        print(f"Error saving to database: {e}")
    
    finally:
        if 'session' in locals():
            session.close()
    
    return