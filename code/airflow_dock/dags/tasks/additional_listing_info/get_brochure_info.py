def get_brochure_info():
    import os
    import re
    import json
    import pandas as pd
    import requests
    from dotenv import load_dotenv
    from pathlib import Path
    from openai import OpenAI

    config_dir = Path("/opt/airflow/config")
    listings_dir = Path("/opt/airflow/data/properties_for_each_brokerage")
    output_bucket = Path("/opt/airflow/data/brochure_info")

    # Function to download a PDF from a URL
    def download_pdf(url):
        response = requests.get(url)
        if response.status_code == 200:
            # return the pdf
            return response.content


    def get_all_properties():
        # Get all csv files in listings_dir and combine into a single dataframe
        all_property_csvs = [pd.read_csv(file) for file in listings_dir.glob("*.csv")]
        all_listings_df = pd.concat(all_property_csvs, ignore_index=True)
        return all_listings_df


    def extract_info_from_brochure(client, assistant, openAI_prompt, brochure_pdf):
        message_file = client.files.create(
                        file=brochure_pdf, 
                        purpose="assistants"
                    )

        # Create a new thread to eliminate information sharing between different brochures
        thread = client.beta.threads.create()

        thread = client.beta.threads.create(
            messages=[
                {
                    "role": "user",
                    "content": openAI_prompt,
                    # Attach the file to the message.
                    "attachments": [
                        {"file_id": message_file.id, 
                        "tools": [{"type": "file_search"}]}
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
    city = 'Edmonton'

    sample_output_dir_path = Path("/opt/airflow/data/brochure_info")
    sample_output_file = sample_output_dir_path / "processed_brochures.json"

    load_dotenv(dotenv_path= config_dir / ".env")
    OpenAI.api_key = os.getenv("OPENAI_API_KEY")

    all_listings_df = get_all_properties()
    assistant_instructions = "You are an expert commercial real estate market analyst. Use your understanding of real estate to retrieve the information I need from the provided property brochures."
    
    client = OpenAI()
    
    assistant = client.beta.assistants.create(
        name="Real Estate Data Extractor",
        instructions=assistant_instructions,
        model="gpt-4o-mini-2024-07-18",
        tools=[{"type": "file_search"}]  # Enables file handling
    )

    # Create a vector store. This will contain embeddings of the document chunks once they are attached in the prompt.
    vector_store = client.beta.vector_stores.create(name="CRE Brochures")

    assistant = client.beta.assistants.update(
        assistant_id=assistant.id,
        tool_resources={"file_search": {"vector_store_ids": [vector_store.id]}},
    )

    # Only keep rows for the specified city
    all_listings_df = all_listings_df[all_listings_df['city'] == city]
    # Only keep rows of all_listings_df if 'bruchure_urls' is not null
    all_listings_df = all_listings_df[all_listings_df['brochure_urls'].notnull()]

    # Get the first 5 rows for testing
    all_listings_df = all_listings_df.head(5) ##################### .head(5) for testing #####################

    processed_brochures_list = []

    for idx, row in all_listings_df.iterrows():
        brochure_url_list = row['brochure_urls']
        uuid = row['uuid']
        
        if brochure_url_list:
            try:
                brochure_url_list = brochure_url_list.strip("[]").replace("'", "").split(", ")
                for brochure_url in brochure_url_list:
                    # Download the PDF
                    brochure_pdf = download_pdf(brochure_url)
                    file_name = os.path.basename(brochure_url)
                    print(f"Downloaded PDF: {file_name}   Calling OpenAI API to extract information...")
                    
                    ##########################################################################################
                    ##                         Information extraction using OpenAI                          ##
                    ##                ----- Uncomment when ready to run paid API calls -----                ##
                    ##########################################################################################
                    # extracted_info = extract_info_from_brochure(client, assistant, openAI_prompt, brochure_pdf)
                    # processed_brochures_list.append({"uuid": uuid, "pdf": {"file_name": file_name, "content": message_content}})
                    ##########################################################################################

            except (ValueError, SyntaxError) as e:
                print(f"Error evaluating brochure URL list: {brochure_url_list} - {e}")
    

    ###### For testing, using a previously generated OpenAI Output ######
    with open(sample_output_file, 'r') as f:
        processed_brochures_list = json.load(f)
    ######################################################################

    with open(sample_output_file, 'w') as f:
        json.dump(processed_brochures_list, f, indent=2)

