def get_zoning_data(gcs_bucket, input_path, output_path):
    import os
    import pandas as pd
    import json
    from sodapy import Socrata
    from dotenv import load_dotenv
    from pathlib import Path
    import geopandas as gpd
    from shapely.geometry import shape, Point
    from google.cloud import storage


    # Configs stored in Google VM instance
    storage_client = storage.Client()
    # Configs stored in Google VM instance
    config_dir = Path("/home/jamesamckinnon1/air_env/configs")

    load_dotenv(dotenv_path= config_dir / ".env")
    coe_username = os.getenv("COE_USERNAME")
    coe_password = os.getenv("COE_PASSWORD")

    coe_client = Socrata("data.edmonton.ca",
                    "Op33anp9RGDX6ywsjFuVs8THM",
                    username=coe_username,
                    password=coe_password)


    zones_info = [row for row in coe_client.get_all("fixa-tstc")]
    zones_df = pd.DataFrame.from_records(zones_info)
    zones_df = zones_df[['zoning', 'description', 'date_ext', 'geometry_multipolygon']]

    zones_df.dropna(subset=['zoning'], inplace=True)
    zones_df['geometry'] = zones_df['geometry_multipolygon'].apply(lambda x: shape(x))
    zones_gdf = gpd.GeoDataFrame(zones_df, geometry='geometry', crs="EPSG:4326")

    # Get all properties from GCS
    bucket = storage_client.bucket(gcs_bucket)
    # List all blobs in the input path
    blobs = list(bucket.list_blobs(prefix=input_path))        
    json_blobs = [blob for blob in blobs if blob.name.endswith('.json')]

    print(f"Found {len(json_blobs)} JSON files in {gcs_bucket}/{input_path}")

    all_listings = []
    for blob in json_blobs:
        content = blob.download_as_string()
        rental_rates = json.loads(content)
        all_listings.extend(rental_rates)


    all_listings_df = pd.DataFrame(all_listings)

    all_listings_df['geometry'] = all_listings_df.apply(
        lambda row: Point(row['longitude'], row['latitude']), axis=1
    )
    properties_gdf = gpd.GeoDataFrame(all_listings_df, geometry='geometry', crs="EPSG:4326")

    
    # Perform a spatial join to find which zone each property falls into
    properties_with_zones_gdf = gpd.sjoin(properties_gdf, zones_gdf, how="left", predicate="within")

    zoning_data_dict = {"properties": {}}

    for idx, row in properties_with_zones_gdf.iterrows():
        uuid = row['uuid']

        property_dict = {
            "uuid": uuid
        }

        # Add zone information
        if not pd.isna(row['zoning']):
            property_dict["zone"] = {
                "zoning": row['zoning'],
                "description": row['description'],
                "geometry": row['geometry_multipolygon'],
            }

        zoning_data_dict["properties"][uuid] = property_dict

    # Save to GCS
    output_blob = bucket.blob(output_path)
    output_blob.upload_from_string(json.dumps(zoning_data_dict, indent=4), content_type='application/json')