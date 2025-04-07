def get_royal_park_listings(gcs_bucket, gcs_path):
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


    url = "https://royalparkrealty.com/wp-admin/admin-ajax.php?a=27334.06846988934"
    payload = {"action": "getProperties"}
    response = requests.post(url, data=payload)


    if response.status_code == 200:
        data = response.json()  
        all_properties_objects = data["properties"][0:5]

        for i, property in enumerate(all_properties_objects):
            print(f"Processing property listing: {i}/{len(all_properties_objects)}")
            link = property["permalink"]
            try:
                response = requests.get(link)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    property_info_list = soup.find('ul', class_='points')

                    if property_info_list:
                        info = {}
                        for li in property_info_list.find_all("li"):
                            key = li.find("span").text
                            value = li.get_text().replace(key, '').strip()
                            key = key.replace(":", "").strip()

                            info[key] = value

                        all_properties_objects[i]["property_type"] = info.get("Type", None)
                    
                    team_members = soup.select('div.property-contacts div.contact-card div.contact-info')

                    if team_members:
                        team_members_dict = {}

                        for member in team_members:
                            # Extract the name, title, phone, and email
                            name_tag = member.select_one('div.contact-info div strong')
                            title_tag = member.select_one('div.contact-info div')  # First div has title                            
                            phone_tag = member.select_one('div.contact-info div a[href^="tel:"]')
                            email_tag = member.select_one('div.contact-info div a[href^="mailto:"]')
                            
                            # Handle missing values and clean data
                            name = name_tag.text.strip() if name_tag else "N/A"
                            title = title_tag.text.strip() if title_tag else "N/A"
                            phone = phone_tag.text.strip() if phone_tag else "N/A"
                            email = email_tag['href'][7:] if email_tag else "N/A"  # Remove "mailto:" from email

                            # Add the extracted information to the team_members_dict
                            team_members_dict[name] = {}
                            team_members_dict[name]["title"] = title.replace(name + " ","").strip()
                            team_members_dict[name]["phone"] = phone
                            team_members_dict[name]["email"] = email
                        
                        all_properties_objects[i]["broker_information"] = team_members_dict
            except Exception as e:
                print(f"Error processing property listing: {i}/{len(all_properties_objects)}")
                print(e)

        listings_df = pd.DataFrame(all_properties_objects)
        
        listings_df.rename(columns={"post_title": "title"}, inplace=True)
        listings_df.rename(columns={"post_content": "property_description"}, inplace=True)
        listings_df.rename(columns={"transaction_type": "sale_or_lease"}, inplace=True)
        listings_df.rename(columns={"brochure": "brochure_urls"}, inplace=True)
        listings_df.rename(columns={"permalink": "listing_url"}, inplace=True)
        listings_df.rename(columns={"thumbnail": "image_url"}, inplace=True)
        listings_df.rename(columns={"post_date": "listing_date"}, inplace=True)
        listings_df.rename(columns={"post_status": "status"}, inplace=True)

        listings_df["city"] = listings_df["address"]
        listings_df["province"] = "Alberta"
        listings_df["country"] = "Canada"
        listings_df["status"] = listings_df["status"].apply(lambda x: x.capitalize() if x != "publish" else "Active")

        listings_df["latitude"] = listings_df["latitude"].astype(float)
        listings_df["longitude"] = listings_df["longitude"].astype(float)

        def extract_img_url(img_str):
            return BeautifulSoup(img_str, 'html.parser').find('img')['src']
        
        listings_df["image_url"] = listings_df["image_url"].apply(lambda x: extract_img_url(x))
        listings_df["last_active_date"] = str(date.today())

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
