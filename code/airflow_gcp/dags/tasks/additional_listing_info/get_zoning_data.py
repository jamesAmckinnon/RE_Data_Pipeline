def get_zoning_data(gcs_bucket, input_path, output_path):
    import sys
    import os
    import pandas as pd
    import json
    import requests
    from pathlib import Path
    import geopandas as gpd
    from shapely.geometry import shape, Point, Polygon
    from google.cloud import storage
    import io


    # Configs stored in Google VM instance
    env = os.getenv("ENV")
    storage_client = storage.Client()

    if env == "GCP":
        zone_colour_scheme_path = "/home/jamesamckinnon1/air_env/configs/tem_current_colour_scheme.json"
    else:
        zone_colour_scheme_path = "/opt/airflow/config/tem_current_colour_scheme.json"

    with open(zone_colour_scheme_path) as f:
        zone_colour_scheme = json.load(f)

    # Get the city zoning data from GCS
    misc_data_bucket = storage_client.bucket("misc-re-data")
    zones_blob = misc_data_bucket.blob('data/zoning_data/zoning_geo_data.csv')
    zones_bytes = zones_blob.download_as_bytes()

    zones_df = pd.read_csv( io.BytesIO(zones_bytes) )
    zones_df.dropna(subset=['zoning'], inplace=True)
    zones_df['geometry'] = zones_df['geometry_multipolygon'].apply(lambda x: shape(eval(x)))
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
                "zone_colour": zone_colour_scheme[row['zoning']],
                "description": row['description'],
                "geometry": row['geometry_multipolygon'],
            }

        zoning_data_dict["properties"][uuid] = property_dict

    # Save to GCS
    output_blob = bucket.blob(output_path)
    output_blob.upload_from_string(json.dumps(zoning_data_dict, indent=4), content_type='application/json')