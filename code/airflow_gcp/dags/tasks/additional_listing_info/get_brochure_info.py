def get_brochure_info(city_name, gcs_bucket, input_path, output_path):
    import os
    import re
    import io
    import json
    import pandas as pd
    import requests
    from dotenv import load_dotenv
    from pathlib import Path
    from openai import OpenAI
    from google.cloud import storage


    env = os.getenv("ENV")
    storage_client = storage.Client()
    testing = False

    if env == "GCP":
        config_dir = Path("/home/jamesamckinnon1/air_env/configs")
        load_dotenv(dotenv_path= config_dir / ".env")
    else:
        config_dir = Path("/opt/airflow/config")

    # Function to download a PDF from a URL
    def download_pdf(url):
        response = requests.get(url)
        if response.status_code == 200:
            return response.content


    def get_all_properties():
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

        return all_listings_df


    def extract_info_from_brochure(client, assistant, openAI_prompt, brochure_pdf, file_name):
        # Wrap bytes in a BytesIO and set a filename ending with .pdf
        brochure_file_obj = io.BytesIO(brochure_pdf)
        brochure_file_obj.name = file_name  # Required so OpenAI knows it's a PDF

        message_file = client.files.create(
            file=brochure_file_obj,
            purpose="assistants"
        )

        # Create a new thread
        thread = client.beta.threads.create(
            messages=[
                {
                    "role": "user",
                    "content": openAI_prompt,
                    "attachments": [
                        {"file_id": message_file.id, "tools": [{"type": "file_search"}]}
                    ],
                }
            ]
        )

        run = client.beta.threads.runs.create_and_poll(
            thread_id=thread.id, assistant_id=assistant.id
        )

        messages = list(client.beta.threads.messages.list(thread_id=thread.id, run_id=run.id))
        message_content = re.sub(r'```json\n(.*?)```', r'\1', messages[0].content[0].text.value, flags=re.DOTALL)

        client.files.delete(message_file.id)

        return message_content


    with open(config_dir / "brochure_info_extraction_prompt.txt", 'r') as f:
        openAI_prompt = f.read()

    #### For testing, using a previously generated OpenAI Output ######
    if testing:
        sample_output_dir_path = Path("/home/jamesamckinnon1/air_env/data/brochure_info")
        sample_output_file = sample_output_dir_path / "processed_brochures.json"
    ###################################################################

    OpenAI.api_key = os.getenv("OPENAI_API_KEY")

    all_listings_df = get_all_properties()
    assistant_instructions = "You are an expert commercial real estate market analyst. Use your understanding of real estate to \
                              retrieve the information I need from the provided property brochures."
    
    client = OpenAI()
    
    assistant = client.beta.assistants.create(
        name="Real Estate Data Extractor",
        instructions=assistant_instructions,
        model="gpt-4o-mini",
        tools=[{"type": "file_search"}]  # Enables file handling
    )

    # Create a vector store. This will contain embeddings of the document chunks once they are attached in the prompt.
    vector_store = client.vector_stores.create(name="CRE Brochures")

    assistant = client.beta.assistants.update(
        assistant_id=assistant.id,
        tool_resources={"file_search": {"vector_store_ids": [vector_store.id]}},
    )

    # Only keep rows for the specified city
    all_listings_df = all_listings_df[all_listings_df['city'] == city_name]
    # Only keep rows of all_listings_df if 'bruchure_urls' is not null
    all_listings_df = all_listings_df[all_listings_df['brochure_urls'].notnull()]

    processed_brochures_list = []

    for idx, row in all_listings_df.iterrows():
        brochure_url_list = row['brochure_urls']
        uuid = row['uuid']
        
        if brochure_url_list:
            try:

                if isinstance(brochure_url_list, str):
                    brochure_url_list = brochure_url_list.strip("[]").replace("'", "").split(", ") 

                for brochure_url in brochure_url_list:
                    # Download the PDF
                    brochure_pdf = download_pdf(brochure_url)
                    file_name = os.path.basename(brochure_url)
                    print(f"Downloaded PDF: {file_name}   Calling OpenAI API to extract information...")
                    
                    ##########################################################################################
                    ##                         Information extraction using OpenAI                          ##
                    ##                     ----- Uncomment to run paid API calls -----                      ##
                    ##########################################################################################
                    extracted_info = extract_info_from_brochure(client, assistant, openAI_prompt, brochure_pdf, file_name)
                    processed_brochures_list.append({"uuid": uuid, "pdf": {"file_name": file_name, "content": extracted_info}})
                    ##########################################################################################

            except Exception as e:
                print(f"Error evaluating brochure URL list: {brochure_url_list} - {e}")
    

    ###### For testing, using a previously generated OpenAI Output ######
    if testing:
        with open(sample_output_file, 'r') as f:
            processed_brochures_list = json.load(f)
    ######################################################################

    # Save combined output to GCS
    bucket = storage_client.bucket(gcs_bucket)
    output_blob = bucket.blob(output_path)
    output_blob.upload_from_string(json.dumps(processed_brochures_list, indent=4), content_type='application/json')
    
    print(f"Saved combined data to gs://{gcs_bucket}/{output_path}")

