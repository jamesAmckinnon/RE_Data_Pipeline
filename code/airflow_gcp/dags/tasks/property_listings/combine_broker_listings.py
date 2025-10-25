def combine_broker_listings(gcs_bucket, properties_input_path, properties_info_input_path):
    import pandas as pd
    import json
    from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.schema import UniqueConstraint
    from sqlalchemy.dialects.postgresql import insert
    import datetime
    from airflow.hooks.base import BaseHook
    from google.cloud import storage


    def get_db_engine():
        conn = BaseHook.get_connection("supabase_db_TP_IPv4")  
        engine = create_engine(
            f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'
        )
        return engine

    # Create SQLAlchemy model for all property listings
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

        __table_args__ = (
            UniqueConstraint('latitude', 'longitude', 'address', name='unqc_lat_lon_address'),
        )
        
        def __repr__(self):
            return f"<PropertyListing(id={self.property_uuid}, title={self.title}, address={self.address})>"

    
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)
    
    print(f"Loading property listings from {gcs_bucket}/{properties_input_path}")
    blobs = list(bucket.list_blobs(prefix=properties_input_path))        
    json_blobs = [blob for blob in blobs if blob.name.endswith('.json')]
    
    print(f"Found {len(json_blobs)} JSON files in {gcs_bucket}/{properties_input_path}")
    all_listings = []
    for blob in json_blobs:
        content = blob.download_as_string()
        rental_rates = json.loads(content)
        all_listings.extend(rental_rates)
    

    def get_sale_or_lease(sale_or_lease):

        if isinstance(sale_or_lease, list):
            sale_or_lease = " ".join(sale_or_lease)

        try:
            if "sale" in sale_or_lease.lower() and "lease" in sale_or_lease.lower():
                sale_or_lease = "sale_or_lease"
            elif "sale" in sale_or_lease.lower():
                sale_or_lease = "sale"
            elif "sublease" in sale_or_lease.lower():
                sale_or_lease = "sublease"
            elif "lease" in sale_or_lease.lower():
                sale_or_lease = "lease"
            return sale_or_lease
        except:
            return ''

    all_listings_df = pd.DataFrame(all_listings)
    all_listings_df['sale_or_lease'] = all_listings_df['sale_or_lease'].apply(get_sale_or_lease)
    all_listings_df['broker_information'] = all_listings_df['broker_information'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
    all_listings_df['brochure_urls'] = all_listings_df['brochure_urls'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

    print(f"Loaded {len(all_listings_df)} properties from {len(json_blobs)} brokerage files")
    
    # Load and merge additional property info files
    info_blobs = list(bucket.list_blobs(prefix=properties_info_input_path))
    json_info_blobs = [blob for blob in info_blobs if blob.name.endswith('.json')]
    print(f"Found {len(json_info_blobs)} property info JSON files")
    print(f"Loaded property info files from {gcs_bucket}/{properties_info_input_path}")
    
    
    # Process each additional property info file
    for info_blob in json_info_blobs:
        file_name = info_blob.name.split('/')[-1].replace('.json', '')
        print(f"Processing {file_name} information")
        
        info_content = info_blob.download_as_string()
        info_data = json.loads(info_content)

        if file_name == "brochure_info":
            for prop in info_data:
                uuid = prop.get('uuid')

                mask = all_listings_df['uuid'] == uuid
                if any(mask):
                    if 'uuid' in prop:
                        del prop['uuid']

                    all_listings_df.loc[mask, "brochure_name"] = prop['pdf']['file_name']
                    all_listings_df.loc[mask, "extracted_brochure_info"] = prop['pdf']['content']


        elif file_name == "osm_data":
            for uuid, prop in info_data["properties"].items():
                mask = all_listings_df['uuid'] == uuid

                if any(mask):
                    if 'uuid' in prop:
                        del prop['uuid']

                    all_listings_df.loc[mask, "nearby_businesses"] = json.dumps(prop["nearby_businesses"])
                    all_listings_df.loc[mask, "nearby_amenities"] = json.dumps(prop["nearby_amenities"])


        elif file_name == "zoning_data":
            for uuid, prop in info_data["properties"].items():
                mask = all_listings_df['uuid'] == uuid

                if any(mask):
                    if 'uuid' in prop:
                        del prop['uuid']

                    if "zone" in prop:
                        all_listings_df.loc[mask, "zoning"] = prop["zone"]["zoning"]
                        all_listings_df.loc[mask, "zoning_description"] = prop["zone"]["description"]
                        all_listings_df.loc[mask, "zone_geometry"] = prop["zone"]["geometry"]
        
        else:
            print(f"Unsupported format for {file_name}, skipping")
    
    
    all_listings_list = all_listings_df.to_dict(orient='records')


    #####################################################
    ########  Save Property Listings to the DB  #########
    #####################################################

    def safe_cast(val, to_type, default):
        try:
            if val is None or pd.isna(val) or val in ["NaN", "nan"]:
                return default
            if to_type is str:
                return str(val)
            return to_type(val)
        except (ValueError, TypeError):
            return default
    
    try:

        # Create database engine
        engine = get_db_engine()
        # Create tables if they don't exist
        Base.metadata.create_all(engine)

        # Create a session to interact with the database
        Session = sessionmaker(bind=engine)
        session = Session()

        num_listings = len(all_listings_list)
        db_property_listings = []

        for i, prop in enumerate(all_listings_list):

            db_property_listings.append({
                "property_uuid": safe_cast(prop.get('uuid'), str, 'Unknown'),
                "address": safe_cast(prop.get('address'), str, 'Unknown'),
                "title": safe_cast(prop.get('title'), str, 'Unknown'),
                "property_description": safe_cast(prop.get('property_description'), str, 'Unknown'),
                "city": safe_cast(prop.get('city'), str, 'Unknown'),
                "province": safe_cast(prop.get('province'), str, 'Unknown'),
                "country": safe_cast(prop.get('country'), str, 'Unknown'),
                "sale_or_lease": safe_cast(prop.get('sale_or_lease'), str, 'Unknown'),
                "price": safe_cast(prop.get('price'), str, 'Unknown'),
                "property_type": safe_cast(prop.get('property_type'), str, 'Unknown'),
                "size": safe_cast(prop.get('size'), str, 'Unknown'),
                "broker_information": safe_cast(prop.get('broker_information'), str, 'Unknown'),
                "status": safe_cast(prop.get('status'), str, 'Unknown'),
                "brochure_urls": safe_cast(prop.get('brochure_urls'), str, 'Unknown'),
                "listing_url": safe_cast(prop.get('listing_url'), str, 'Unknown'),
                "image_url": safe_cast(prop.get('image_url'), str, 'Unknown'),
                "listing_date": safe_cast(prop.get('listing_date'), str, 'Unknown'),
                "last_active_date": safe_cast(prop.get('last_active_date'), str, 'Unknown'),
                "nearby_businesses": safe_cast(prop.get('nearby_businesses'), str, 'Unknown'),
                "nearby_amenities": safe_cast(prop.get('nearby_amenities'), str, 'Unknown'),
                "zoning": safe_cast(prop.get('zoning'), str, 'Unknown'),
                "zoning_description": safe_cast(prop.get('zoning_description'), str, 'Unknown'),
                "zone_geometry": safe_cast(prop.get('zone_geometry'), str, 'Unknown'),
                "brochure_name": safe_cast(prop.get('brochure_name'), str, 'Unknown'),
                "extracted_brochure_info": safe_cast(prop.get('extracted_brochure_info'), str, 'Unknown'),
                "latitude": safe_cast(prop.get('latitude'), float, 0.0),
                "longitude": safe_cast(prop.get('longitude'), float, 0.0)
            })


        stmt = insert(PropertyListing).values(db_property_listings)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=['latitude', 'longitude', 'address']
        )
        result = session.execute(stmt)

        session.commit()
        if result.rowcount != 0:
            print(f"Successfully saved {result.rowcount} property listings to the database.")
        else:
            print("No new property listings to save to the database.")
    
    except Exception as e:
        if 'session' in locals():
            session.rollback()
        print(f"Error saving to database: {e}")
    
    finally:
        if 'session' in locals():
            session.close()

        # Delete blobs from GCS (both listings and info files)
        for blob in json_info_blobs:
            try:
                blob.delete()
            except Exception as delete_err:
                print(f"Failed to delete {blob.name}: {delete_err}")
        
    return