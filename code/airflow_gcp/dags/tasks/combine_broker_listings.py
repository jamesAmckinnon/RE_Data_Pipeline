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


def combine_broker_listings(gcs_bucket, properties_input_path, properties_info_input_path, output_path):
    import pandas as pd
    import json
    from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
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
        zone_colour = Column(String)
        zone_geometry = Column(String)
        brochure_name = Column(String)
        extracted_brochure_info = Column(String)

        latitude = Column(Float)
        longitude = Column(Float)

        
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
    
    all_listings_df = pd.DataFrame(all_listings)
    all_listings_df['sale_or_lease'] = all_listings_df['sale_or_lease'].apply(get_sale_or_lease)
    all_listings_df['broker_information'] = all_listings_df['broker_information'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
    all_listings_df['brochure_urls'] = all_listings_df['brochure_urls'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

    print(f"Loaded {len(all_listings_df)} properties from {len(json_blobs)} brokerage files")
    
    # Load and merge additional property info files
    print(f"Loading property info files from {gcs_bucket}/{properties_info_input_path}")
    info_blobs = list(bucket.list_blobs(prefix=properties_info_input_path))
    json_info_blobs = [blob for blob in info_blobs if blob.name.endswith('.json')]
    
    print(f"Found {len(json_info_blobs)} property info JSON files")
    
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
                    if 'uuid' in prop_info:
                        del prop_info['uuid']

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
                        all_listings_df.loc[mask, "zone_colour"] = prop["zone"]["zone_colour"]
                        all_listings_df.loc[mask, "zone_geometry"] = prop["zone"]["geometry"]
        
        else:
            print(f"Unsupported format for {file_name}, skipping")
    

    print(f"Saving combined data with {len(all_listings_df.columns)} columns")
    
    all_listings_list = all_listings_df.to_dict(orient='records')

    combined_json = all_listings_df.to_json(orient='records')
    
    # Upload to GCS
    output_blob = bucket.blob(output_path)
    output_blob.upload_from_string(combined_json, content_type='application/json')
    
    print(f"Successfully saved combined data to gs://{gcs_bucket}/{output_path}")
    
    #####################################################
    ########  Save Property Listings to the DB  #########
    #####################################################
    
    try:

        # Create database engine
        engine = get_db_engine()
        # Create tables if they don't exist
        Base.metadata.create_all(engine)
        
        # Create a session to interact with the database
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Convert the data to a list of PropertyListing objects
        db_property_listings = []
        for prop in all_listings_list:
            db_property_listings.append(
                PropertyListing(
                    property_uuid=prop.get('uuid', ''),
                    address=prop.get('address', ''),
                    title=prop.get('title', ''),
                    property_description=prop.get('property_description', ''),
                    city=prop.get('city', ''),
                    province=prop.get('province', ''),
                    country=prop.get('country', ''),
                    sale_or_lease=prop.get('sale_or_lease', ''),
                    price=prop.get('price', ''),
                    property_type=prop.get('property_type', ''),
                    size=prop.get('size', ''),
                    broker_information=prop.get('broker_information', ''),
                    status=prop.get('status', ''),
                    brochure_urls=prop.get('brochure_urls', ''),
                    listing_url=prop.get('listing_url', ''),
                    image_url=prop.get('image_url', ''),
                    listing_date=prop.get('listing_date', ''),
                    last_active_date=prop.get('last_active_date', ''),
                    nearby_businesses=prop.get('nearby_businesses', ''),
                    nearby_amenities=prop.get('nearby_amenities', ''),
                    zoning=prop.get('zoning', ''),
                    zoning_description=prop.get('zoning_description', ''),
                    zone_colour=prop.get('zone_colour', ''),
                    zone_geometry=prop.get('zone_geometry', ''),
                    brochure_name=prop.get('brochure_name', ''),
                    extracted_brochure_info=prop.get('extracted_brochure_info', ''),
                    latitude=prop.get('latitude', 0.0),
                    longitude=prop.get('longitude', 0.0),
                )
            )
        
        # Add all rental rates to the database
        session.add_all(db_property_listings)
        session.commit()
        print(f"Successfully saved {len(db_property_listings)} property listings to the database")
    
    except Exception as e:
        if 'session' in locals():
            session.rollback()
        print(f"Error saving to database: {e}")
    
    finally:
        if 'session' in locals():
            session.close()
    
    return f"gs://{gcs_bucket}/{output_path}"