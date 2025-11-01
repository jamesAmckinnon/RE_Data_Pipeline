def aggregate_rental_rates(gcs_bucket, input_path, center_lat, center_lon, grid_size=32000, cell_size=500):
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import Point, Polygon
    import json
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

    class AvgRentalRate(Base):
        __tablename__ = 'avg_rent_listings'
        
        avg_rent_unit_id = Column(Integer, primary_key=True)
        grid_coordinates = Column(String)
        avg_rental_rate = Column(Float)
        num_properties = Column(Integer)
        standard_deviation = Column(Float)
        bedrooms = Column(Integer)
        
        def __repr__(self):
            return f"<AvgRentalRate(avg_rental_rate={self.avg_rental_rate}, num_properties={self.num_properties}, standard_deviation={self.standard_deviation})>"
        

    def get_rental_data():
        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(gcs_bucket)
        
        # Get the input blob
        blob = bucket.blob(input_path)
        
        if not blob.exists():
            print(f"No rental data file found at gs://{gcs_bucket}/{input_path}. Skipping aggregation.")
            return None
        
        # Download and parse the JSON content
        content = blob.download_as_string()
        rental_data = json.loads(content)
        
        rental_df = pd.DataFrame(rental_data)
        
        # Handle potential numeric conversion issues
        for col in ['latitude', 'longitude', 'rental_rate']:
            if col in rental_df.columns and rental_df[col].dtype == 'object':
                rental_df[col] = pd.to_numeric(rental_df[col], errors='coerce')
        
        # Create geometries
        rental_df['geometry'] = rental_df.apply(
            lambda row: Point(row['longitude'], row['latitude']) 
            if pd.notnull(row['longitude']) and pd.notnull(row['latitude']) 
            else None, 
            axis=1
        )
        
        # Drop rows with invalid geometries
        rental_df = rental_df.dropna(subset=['geometry'])
        
        return gpd.GeoDataFrame(rental_df, geometry='geometry', crs="EPSG:4326")


    def create_grid(center_point, grid_size, cell_size):
        minx = center_point.x - grid_size / 2
        miny = center_point.y - grid_size / 2
        maxx = center_point.x + grid_size / 2
        maxy = center_point.y + grid_size / 2
        
        grid_cells = []
        x = minx
        while x < maxx:
            y = miny
            while y < maxy:
                grid_cells.append(Polygon([(x, y), (x + cell_size, y), (x + cell_size, y + cell_size), (x, y + cell_size)]))
                y += cell_size
            x += cell_size

        return gpd.GeoDataFrame(geometry=grid_cells, crs="EPSG:32612")

    # Get rental data from GCS
    rentals_gdf = get_rental_data()
    
    if rentals_gdf is None or rentals_gdf.empty:
        print("No valid rental data available for processing.")
        return

    print(f"columns in rentals_gdf: {rentals_gdf.columns.tolist()}")
    
    # Convert to UTM coordinates for accurate spatial calculations
    try:
        rentals_gdf = rentals_gdf.to_crs("EPSG:32612")
    except Exception as e:
        print(f"Error converting coordinates: {e}")
        print("Sample data from rentals_gdf:")
        print(rentals_gdf[['latitude', 'longitude']].head())
        return
    
    # Create center point and transform to UTM
    center_point = gpd.GeoSeries([Point(center_lon, center_lat)], crs="EPSG:4326").to_crs("EPSG:32612").iloc[0]
    
    # Create grid
    grid_gdf = create_grid(center_point, grid_size, cell_size)
    
    print(f"Created grid with {len(grid_gdf)} cells")
    

    # Find all rental listings that fall within each grid cell and average them by number of bedrooms
    grid_rent_mapping = {}

    for idx, cell in grid_gdf.iterrows():
        listings_in_cell = rentals_gdf[rentals_gdf.within(cell.geometry)]
        if not listings_in_cell.empty:
            # Group by number of bedrooms
            for bedrooms, group in listings_in_cell.groupby('bedrooms'):
                rents_in_group = pd.to_numeric(group['rental_rate'], errors='coerce').dropna()
                if not rents_in_group.empty:
                    avg_rent = rents_in_group.mean()
                    num_properties = len(rents_in_group)
                    std_dev = rents_in_group.std() if len(rents_in_group) > 1 else 0

                    # Use a tuple (cell idx, bedrooms) as the key
                    grid_rent_mapping[(idx, bedrooms)] = {
                        "grid_coordinates": str(list(cell.geometry.exterior.coords)),
                        "bedrooms": int(bedrooms),
                        "avg_rental_rate": float(avg_rent),
                        "num_properties": int(num_properties),
                        "standard_deviation": float(std_dev) if not pd.isna(std_dev) else 0.0
                    }

    print(f"Found {len(grid_rent_mapping)} grid cell/bedroom combinations with rental properties")

    try:
        # Create database engine
        engine = get_db_engine()
        # Create tables if they don't exist
        Base.metadata.create_all(engine)
        
        # Create a session to interact with the database
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Convert the data to a list of AvgRentalRate objects
        db_rental_rates = []
        for (idx, bedrooms), rate in grid_rent_mapping.items():
            db_rental_rates.append(
                AvgRentalRate(
                    avg_rental_rate=rate.get('avg_rental_rate'),
                    grid_coordinates=rate.get('grid_coordinates'),
                    num_properties=rate.get('num_properties'),
                    standard_deviation=rate.get('standard_deviation'),
                    bedrooms=rate.get('bedrooms')
                )
            )
        
        # Add all rental rates to the database
        session.add_all(db_rental_rates)
        session.commit()
        print(f"Successfully saved {len(db_rental_rates)} aggregated rental rates to the database")
    
    except Exception as e:
        if 'session' in locals():
            session.rollback()
        print(f"Error in database operation: {e}")
    
    finally:
        if 'session' in locals():
            session.close()
    
    return


