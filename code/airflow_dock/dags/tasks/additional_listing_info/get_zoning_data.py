def get_zoning_data():
    import sys
    import pandas as pd
    import json
    import requests
    from pathlib import Path
    import geopandas as gpd
    from shapely.geometry import shape, Point, Polygon

    listings_dir = Path("/opt/airflow/data/properties_for_each_brokerage")
    output_bucket = Path("/opt/airflow/data/additional_property_info")
    zone_colour_scheme_path = Path("/opt/airflow/config/tem_current_colour_scheme.json")
    data_dir = Path("/opt/airflow/data")

    with open(zone_colour_scheme_path) as f:
        zone_colour_scheme = json.load(f)

    zones_df = pd.read_csv(data_dir / 'zoning_data/zoning_geo_data.csv')
    zones_df.dropna(subset=['zoning'], inplace=True)
    zones_df['geometry'] = zones_df['geometry_multipolygon'].apply(lambda x: shape(eval(x)))
    zones_gdf = gpd.GeoDataFrame(zones_df, geometry='geometry', crs="EPSG:4326")

    all_property_csvs = [pd.read_csv(file) for file in listings_dir.glob("*.csv")]
    all_listings_df = pd.concat(all_property_csvs, ignore_index=True).head(5) #####################

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

    with open(output_bucket / "zoning_data_dict.json", "w") as f:
        json.dump(zoning_data_dict, f, indent=4)