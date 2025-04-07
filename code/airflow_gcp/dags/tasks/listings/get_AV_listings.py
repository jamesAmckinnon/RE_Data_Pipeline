def get_AV_listings(gcs_bucket, gcs_path):
    import sys
    import pandas as pd
    import json
    import requests
    from bs4 import BeautifulSoup
    from datetime import date
    import uuid
    from google.cloud import storage


    # Configs stored in Google VM instance
    output_schema_path = "/home/jamesamckinnon1/air_env/configs/brokerage_listing_schemas.json"
    with open(output_schema_path, 'r') as f:
        output_schema =  json.load(f)
    valid_schema = True

    # URL to scrape
    url = "https://5igwwa7oi7.execute-api.us-east-1.amazonaws.com/data?entity=website&status=active,escrow,closed"
    headers = {"x-api-key": "10f000ab4a4388a50b0acd8c98b3dfad"}
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:

        data = response.json()
        all_properties_objects = data["items"][0:5]

        for i, property in enumerate(all_properties_objects):
            print(f"Processing property listing: {i}/{len(all_properties_objects)}")
            link = property["external_url"]
            try:
                response = requests.get(link)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    property_info_list = soup.find('div', class_='property-info-list')

                    if property_info_list:
                        info = {}
                        for li in property_info_list.find_all("li"):
                            spans = li.find_all("span")
                            if len(spans) == 2:
                                key, value = spans[0].text.replace(":","").strip().lower(), spans[1].text.strip()
                                info[key] = value

                        def get_first_available(info, keys):
                            for key in keys:
                                if key in info:
                                    return info[key]
                            return "Unknown"  

                        all_properties_objects[i]["property_type"] = get_first_available(info, ["property type"])
                        all_properties_objects[i]["price"]         = get_first_available(info, ["price", "price/acre", "asking price", "sale price", "asking price per acre", "price per acre", "price per a square foot", "listing price"])
                        all_properties_objects[i]["size"]          = get_first_available(info, ["building size (sf)", "building size", "size (sf)", "total portfolio leasable area", "building square footage", "size (ac)", "size", "land size", "total land area", "site size", "total portfolio site area"])

                    team_members = soup.find('div', class_="leasing__brokerage-team-members")

                    if team_members:
                        team_members_dict = {}

                        for member in team_members.find_all('div', class_="team-member"):
                            member_name_tag = member.find('h4', class_="team-member__name") or member.find('a', class_="team-member__name")
                            member_name = member_name_tag.text if member_name_tag else None
                            if member_name != None:
                                team_members_dict[member_name] = {}

                                team_members_dict[member_name]["title"] = member.find('div', class_="team-member__job").text.replace("\n", "").strip()
                                team_members_dict[member_name]["phone"] = member.find('div', class_="team-member__phone").text.replace("\n", "").strip()

                        all_properties_objects[i]["broker_information"] = team_members_dict

                    brochures = soup.find_all("div", class_="availability__row-buttons")

                    if brochures:
                        brochure_list = []

                        for brochure in brochures:
                            brochure_a_tag = brochure.find('a', class_="link__text--hoveraccent")
                            if brochure_a_tag:
                                href = brochure_a_tag.get('href')
                                brochure_list.append(href)

                        all_properties_objects[i]["brochure_urls"] = brochure_list
            except Exception as e:
                print(f"Error processing property: {i}/{len(all_properties_objects)}")
                print(e)

        listings_df = pd.DataFrame(all_properties_objects)

        listings_df.rename(columns={"name": "title"}, inplace=True)
        listings_df.rename(columns={"state_long": "province"}, inplace=True)
        listings_df.rename(columns={"meta_description": "property_description"}, inplace=True)
        listings_df.rename(columns={"transaction": "sale_or_lease"}, inplace=True)
        listings_df.rename(columns={"external_url": "listing_url"}, inplace=True)
        listings_df.rename(columns={"image_path": "image_url"}, inplace=True)
        listings_df.rename(columns={"on_market_at": "listing_date"}, inplace=True)

        listings_df["latitude"] = listings_df["location"].apply(lambda x: x["lat"])
        listings_df["longitude"] = listings_df["location"].apply(lambda x: x["lng"])
        listings_df["image_url"] = listings_df["image_url"].apply(lambda x: "https://d3k1yame0apvip.cloudfront.net/" + str(x))
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
        
        ################################# TODO #######################################
        # Should retrieve existing data, check if the property already exists, and
        # also if it did exist but does not now, there should be a column to say what 
        # day was the last day it was available and put another column to indicate
        # that it is not active now. 
        ##############################################################################

    else:
        print(f"Error: {response.status_code}")