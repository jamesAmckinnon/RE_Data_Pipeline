def archive_delisted_properties(gcs_bucket, properties_input_path):
    import pandas as pd
    import json
    from sqlalchemy import create_engine, Column, String, Float
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.dialects.postgresql import insert
    from sqlalchemy import tuple_
    from airflow.hooks.base import BaseHook
    from google.cloud import storage

    def get_db_engine():
        conn = BaseHook.get_connection("supabase_db_TP_IPv4")  
        engine = create_engine(
            f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'
        )
        return engine

    Base = declarative_base()

    class PropertyListing(Base):
        __tablename__ = 'property_listings'
        property_uuid = Column(String, primary_key=True)
        address = Column(String)
        title = Column(String)
        property_description = Column(String)
        city = Column(String)
        province = Column(String)
        country = Column(String)
        sale_or_lease = Column(String)
        price = Column(String)
        property_type = Column(String)
        size = Column(String)
        broker_information = Column(String)
        status = Column(String)
        brochure_urls = Column(String)
        listing_url = Column(String)
        image_url = Column(String)
        listing_date = Column(String)
        last_active_date = Column(String)
        nearby_businesses = Column(String)
        nearby_amenities = Column(String)
        zoning = Column(String)
        zoning_description = Column(String)
        zone_geometry = Column(String)
        brochure_name = Column(String)
        extracted_brochure_info = Column(String)
        latitude = Column(Float)
        longitude = Column(Float)

    class DelistedPropertyListing(Base):
        __tablename__ = 'delisted_property_listings'
        property_uuid = Column(String, primary_key=True)
        address = Column(String)
        title = Column(String)
        property_description = Column(String)
        city = Column(String)
        province = Column(String)
        country = Column(String)
        sale_or_lease = Column(String)
        price = Column(String)
        property_type = Column(String)
        size = Column(String)
        broker_information = Column(String)
        status = Column(String)
        brochure_urls = Column(String)
        listing_url = Column(String)
        image_url = Column(String)
        listing_date = Column(String)
        last_active_date = Column(String)
        nearby_businesses = Column(String)
        nearby_amenities = Column(String)
        zoning = Column(String)
        zoning_description = Column(String)
        zone_geometry = Column(String)
        brochure_name = Column(String)
        extracted_brochure_info = Column(String)
        latitude = Column(Float)
        longitude = Column(Float)

    # Step 1: Load current properties from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)
    blobs = list(bucket.list_blobs(prefix=properties_input_path))        
    json_blobs = [blob for blob in blobs if blob.name.endswith('.json')]

    all_listings = []
    for blob in json_blobs:
        content = blob.download_as_string()
        listings = json.loads(content)
        all_listings.extend(listings)

    current_keys = {
        (p.get('latitude'), p.get('longitude'), p.get('address'))
        for p in all_listings
        if p.get('latitude') is not None and p.get('longitude') is not None and p.get('address')
    }

    engine = get_db_engine()
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    existing_listings = session.query(PropertyListing).all()
    existing_keys = {
        (p.latitude, p.longitude, p.address) 
        for p in existing_listings
        if p.latitude is not None and p.longitude is not None and p.address 
    }

    delisted_keys = existing_keys - current_keys
    if not delisted_keys:
        print("No delisted properties detected.")
        session.close()
        return

    print(f"Archiving {len(delisted_keys)} delisted properties.")

    delisted_props = [p for p in existing_listings if (p.latitude, p.longitude, p.address) in delisted_keys]
    print(f"Delisted properties: {[p.address for p in delisted_props]}")

    # delisted_props = [p for p in existing_listings if (p.latitude, p.longitude, p.address) in delisted_keys]

    delisted_records = [
        {col.name: getattr(p, col.name) for col in DelistedPropertyListing.__table__.columns}
        for p in delisted_props
    ]

    # should add 2 less to db but there should still be all properties in db
    # will then run again. combined_ should not add any properties to db but in 
    # this script ill remove a scraped property to test if that property in the 
    # db will be sent to archive. Shoul dthen have one less property in property_listings 
    # and one property in archive.

    # Insert into archive table (ignore if already archived)
    stmt = insert(DelistedPropertyListing).values(delisted_records).on_conflict_do_nothing()
    result = session.execute(stmt)

    session.commit()  # commit archive inserts first

    if delisted_props:  # If we have properties to delete
        delete_keys = [(p.latitude, p.longitude, p.address) for p in delisted_props]
        deleted_count = session.query(PropertyListing).filter(
            tuple_(PropertyListing.latitude, PropertyListing.longitude, PropertyListing.address).in_(delete_keys)
        ).delete(synchronize_session=False)
        session.commit()
        print(f"Deleted {deleted_count} delisted properties from active listings.")
    else:
        print("No delisted properties to delete.")
    
    session.close()

    # Delete listing blobs from GCS 
    for blob in json_blobs:
        try:
            blob.delete()
        except Exception as delete_err:
            print(f"Failed to delete {blob.name}: {delete_err}")