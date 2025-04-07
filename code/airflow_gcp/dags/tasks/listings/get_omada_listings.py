def get_omada_listings(gcs_bucket, gcs_path):
    import sys
    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
    import re
    import json
    from datetime import date
    import uuid
    from google.cloud import storage


    # Configs stored in Google VM instance
    output_schema_path = "/home/jamesamckinnon1/air_env/configs/brokerage_listing_schemas.json"
    with open(output_schema_path, 'r') as f:
        output_schema =  json.load(f)
    valid_schema = True


    url = "https://omada-cre.com/wp-json/wp/v2/listing"
    response = requests.get(url)


    if response.status_code == 200:

        all_properties_objects = response.json()[0:5]

        for i, property in enumerate(all_properties_objects):
            print(f"Processing property listing: {i}/{len(all_properties_objects)}")
            link = property["link"]
            try:
                response = requests.get(link)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')

                    team_members = soup.find('div', id="listing-realtor-bios")

                    if team_members:
                        team_members_dict = {}

                        for member in team_members.find_all('ul', class_="bio-card-info-list"):
                            name_tag  = member.select_one('span.bio-card-name a.link-clean')
                            title_tag = member.select_one('span.bio-card-position')
                            phone_tag = member.find('a', title="Phone Number")
                            email_tag = member.find('a', title="Email")

                            member_name  = name_tag.text.title()  if name_tag  else "N/A"
                            member_title = title_tag.text         if title_tag else "N/A"
                            member_phone = phone_tag.text.strip() if phone_tag else "N/A"
                            member_email = email_tag.text.strip() if email_tag else "N/A"

                            team_members_dict[member_name] = {}
                            team_members_dict[member_name]["title"] = member_title
                            team_members_dict[member_name]["phone"] = member_phone
                            team_members_dict[member_name]["email"] = member_email

                        all_properties_objects[i]["broker_information"] = team_members_dict

                    listing_features = soup.find('div', id="listing-feature-omada")

                    if listing_features:
                        strong_tags = listing_features.find_all('strong')

                        # Search for the word "price" and extract the text that comes after it
                        for tag in strong_tags:
                            if "price" in tag.text.lower():
                                price = tag.next_sibling.strip() if tag.next_sibling else "N/A"
                                all_properties_objects[i]["price"] = price
                            else:
                                all_properties_objects[i]["price"] = "Unknown"

                    brochure_link = soup.find("a", class_="brochure-doc-link")

                    if brochure_link:
                        all_properties_objects[i]["brochure_urls"] = brochure_link.get("href")
            except Exception as e:
                print(f"Error processing property listing: {i}/{len(all_properties_objects)}")
                print(e)

        listings_df = pd.DataFrame(all_properties_objects)

        listings_df.rename(columns={"_listing_address": "address"}, inplace=True)
        listings_df.rename(columns={"_listing_city": "city"}, inplace=True)
        listings_df.rename(columns={"_listing_state": "province"}, inplace=True)
        listings_df.rename(columns={"_listing_latitude": "latitude"}, inplace=True)
        listings_df.rename(columns={"_listing_longitude": "longitude"}, inplace=True)
        listings_df.rename(columns={"link": "listing_url"}, inplace=True)
        listings_df.rename(columns={"featured_image_src": "image_url"}, inplace=True)
        listings_df.rename(columns={"date": "listing_date"}, inplace=True)

        listings_df["title"] = listings_df["title"].apply(lambda x: x["rendered"])
        listings_df["country"] = "Canada"
        listings_df["latitude"] = listings_df["latitude"].astype(float)
        listings_df["longitude"] = listings_df["longitude"].astype(float)

        def join_names(key, x):
            if key in x:
                return ", ".join([stat["name"] for stat in x[key]])
            else:
                return None
        listings_df["sale_or_lease"] = listings_df["pti_taxonomies"].apply(lambda x: join_names("status", x))
        listings_df["property_type"] = listings_df["pti_taxonomies"].apply(lambda x: join_names("property-types", x))
        
        def extract_size(row):
            if row["_listing_sqft_min"] != None and row["_listing_sqft_max"] != None:
                return row["_listing_sqft_min"] + " - " + row["_listing_sqft_max"] + " SF"
            elif row["_listing_sqft_min"] != None:
                return row["_listing_sqft_min"] + " SF"
            elif row["_listing_acre_min"] != None and row["_listing_acre_max"] != None:
                return row["_listing_acre_min"] + " - " + row["_listing_acre_max"] + " Acres"
            elif row["_listing_acre_min"] != None:
                return row["_listing_acre_min"] + " Acres"
            else:
                return "Unknown"
        
        listings_df["size"] = listings_df.apply(lambda x: extract_size(x), axis=1)
        listings_df["status"] = "Active" # It seems like all listings are active if still on site
        listings_df["last_active_date"] = str(date.today())

        def extract_text(txt_str): 
            return BeautifulSoup(txt_str, 'html.parser').get_text(separator="\n")
        listings_df["property_description"] = listings_df["_listing_highlights_omada"].apply(lambda x: extract_text(x))

        listings_df['uuid'] = [uuid.uuid4().hex  for _ in range(len(listings_df))]

        # Validate and filter output DF using schema
        try:
            listings_df = listings_df[output_schema["property_listing_schema"]]
        except Exception as e:
            valid_schema = False
            print("Output schema is invalid:  ", e)

        if valid_schema:
            rent_data = listings_df.to_dict(orient="records")
            print("\n============ Non-nan percentages ============ \n\n", (listings_df.count() / len(listings_df))*100, "\n")

            try:
                storage_client = storage.Client()
                bucket = storage_client.bucket(gcs_bucket)
                blob = bucket.blob(gcs_path)
                blob.upload_from_string(json.dumps(rent_data, indent=4), content_type="application/json")
            except Exception as e:
                print("Error uploading to GCS: ", e)