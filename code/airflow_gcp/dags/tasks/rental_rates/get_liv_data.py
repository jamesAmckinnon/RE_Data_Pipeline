def get_liv_data(gcs_bucket, gcs_path):
    import requests
    import json
    import uuid
    from google.cloud import storage
    import os


    url = "https://nemesis-prod.liv.rent/graphql"
    headers = {}
    payload = {
      "variables": {
          "input": {
              "featured": 100,
              "page": 1,
              "page_size": 3000,
              "housing_types": [],
              "unit_types": [],
              "bedroom_count": [],
              "cities": ["Edmonton"],
              "sort": {
                  "sort_by": "SUGGESTED",
                  "sort_type": "ASC"
              }
          }
      },
      "query": """
      fragment mapBuildingListFields on Listing {
        bathrooms
        bedrooms
        gr_min_price
        gr_max_price
        gr_unit
        gr_count
        allow_applications
        public_viewing
        hide_unit_number
        unit_type_txt_id
        unit_type_scope_txt_id
        cover_photo_aws_s3_key
        listing_id
        price
        price_frequency
        is_hidden
        state_machine
        verified_state_machine
        furnished
        size
        unit_files {
          aws_s3_key
          position
          tag
          __typename
        }
        landlords {
          identity_verified_state
          __typename
        }
        __typename
      }

      fragment mapListViewFields on FullListing {
        street_name
        building_type
        building_name
        building_id
        full_street_name
        city
        state
        zip
        location {
          lat
          lon
          __typename
        }
        bathrooms
        bedrooms
        size
        gr_min_size
        gr_min_price
        gr_max_price
        allow_applications
        public_viewing
        gr_unit
        hide_unit_number
        unit_type_txt_id
        unit_type_scope_txt_id
        cover_photo_aws_s3_key
        listing_id
        price
        price_frequency
        is_hidden
        state_machine
        verified_state_machine
        furnished
        unit_files {
          aws_s3_key
          position
          tag
          __typename
        }
        landlords {
          identity_verified_state
          __typename
        }
        __typename
      }

      query ($input: ListSearchInput!) {
        listSearch {
          buildings(input: $input) {
            metadata {
              page
              page_size
              total_count
              listings_count
              __typename
            }
            feed {
              building_id
              street_name
              building_type
              full_street_name
              city
              state
              zip
              building_name
              location {
                lat
                lon
                __typename
              }
              building_files {
                aws_s3_key
                tag
                __typename
              }
              listings {
                ...mapBuildingListFields
                __typename
              }
              listing_count
              __typename
            }
            __typename
          }
          featured(input: $input) {
            ...mapListViewFields
            __typename
          }
          __typename
        }
      }
      """
    }



    # Make the API request to scrape data
    response = requests.post(url, json=payload, headers=headers)
    
    # Check if response was successful
    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
    
    # Extract the listings data
    try:
        listings = response.json()["data"]["listSearch"]["buildings"]["feed"]
    except (KeyError, IndexError) as e:
        raise Exception(f"Error parsing API response: {e}. Response: {response.text[:200]}")
    
    # Process the listings
    rent_data = []
    for listing in listings:
        property_uuid = uuid.uuid4().hex
        listing_temp = {
            "uuid": property_uuid,
            "building_name": listing["building_name"],
            "building_type": listing["building_type"],
            "address": listing["full_street_name"],
            "city": listing["city"],
            "province": listing["state"],
            "latitude": listing["location"]["lat"],
            "longitude": listing["location"]["lon"],
        }
        for unit in listing["listings"]:
            unit_info = listing_temp.copy()
            unit_info["rental_rate"] = unit["price"]
            unit_info["bedrooms"] = unit["bedrooms"]
            unit_info["bathrooms"] = unit["bathrooms"]
            unit_info["size"] = unit["size"]
            rent_data.append(unit_info)
    
    print(f"Saving {len(rent_data)} units to GCS: {gcs_bucket}/{gcs_path}")
    
    # Save data to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(json.dumps(rent_data, indent=4), content_type="application/json")
    
    return f"gs://{gcs_bucket}/{gcs_path}"
