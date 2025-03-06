def aggregate_rental_rates(center_lat, center_lon, grid_size=32000, cell_size=500):
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import Point, Polygon
    import json
    from pathlib import Path
    from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Table, MetaData
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    import datetime
    from airflow.hooks.base import BaseHook

    input_bucket = Path("/opt/airflow/data/combined_rental_rates")
    output_bucket = Path("/opt/airflow/data/aggregated_rental_rates")

    # Get database connection from Airflow connections
    def get_db_engine():
        conn = BaseHook.get_connection("postgres_localhost")
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
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
        created_at = Column(DateTime, default=datetime.datetime.now(datetime.timezone.utc))
        
        def __repr__(self):
            return f"<AvgRentalRate(avg_rental_rate={self.avg_rental_rate}, num_properties=${self.num_properties}, standard_deviation={self.standard_deviation})>"
        

    def get_rental_data():
        json_files = list(input_bucket.glob("*.json"))
        
        if not json_files:  # No files found
            print("No rental data files found. Skipping aggregation.")
            return None  # Don't proceed if no data is available
        
        rental_data_path = json_files[0]  # Take the first JSON file
        with open(rental_data_path, "r") as f:
            rental_data = json.load(f)
        
        rental_df = pd.DataFrame(rental_data)
        rental_df['geometry'] = rental_df.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
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

    rentals_gdf = get_rental_data()

    if rentals_gdf is None:
        return
    else:
        rentals_gdf = rentals_gdf.to_crs("EPSG:32612")
    
    center_point = gpd.GeoSeries([Point(center_lon, center_lat)], crs="EPSG:4326").to_crs("EPSG:32612").iloc[0]
    grid_gdf = create_grid(center_point, grid_size, cell_size)
    

    # Find all rental listings that fall within each grid cell and average them
    grid_rent_mapping = {}
    
    for idx, cell in grid_gdf.iterrows():
        rents_in_cell = rentals_gdf[rentals_gdf.within(cell.geometry)]['rental_rate']
        if not rents_in_cell.empty:
            avg_rent = rents_in_cell.mean()
            num_properties = len(rents_in_cell)
            std_dev = rents_in_cell.std()

            grid_rent_mapping[idx] = {
                "grid_coordinates": str(list(cell.geometry.exterior.coords)),
                "avg_rental_rate": avg_rent,
                "num_properties": num_properties,
                "standard_deviation": std_dev
            }
    
    output_path = output_bucket / "aggregated_rental_rates.json"
    with open(output_path, "w") as f:
        json.dump(grid_rent_mapping, f, indent=4)

    # Create database engine
    engine = get_db_engine()
    # Create tables if they don't exist
    Base.metadata.create_all(engine)
    
    # Create a session to interact with the database
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Convert the data to a list of AvgRentalRate objects
        db_rental_rates = []
        for idx, rate in grid_rent_mapping.items():

            db_rental_rates.append(
                AvgRentalRate(
                    avg_rental_rate=rate.get('avg_rental_rate', None),
                    grid_coordinates=rate.get('grid_coordinates', None),
                    num_properties=rate.get('num_properties', None),
                    standard_deviation=rate.get('standard_deviation', None),
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
    

