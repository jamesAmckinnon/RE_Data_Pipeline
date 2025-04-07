def get_REIT_report_data(gcs_bucket, gcs_input_path):
    import os
    import re
    import json
    import pandas as pd
    import requests
    from dotenv import load_dotenv
    from pathlib import Path
    from openai import OpenAI
    from io import BytesIO
    import fitz 
    import re
    import uuid
    from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    import datetime
    from airflow.hooks.base import BaseHook
    from google.cloud import storage
    import io

    # Configs stored in Google VM instance
    config_dir = Path("/home/jamesamckinnon1/air_env/configs")

    # Get database connection from Airflow connections
    def get_db_engine():
        conn = BaseHook.get_connection("supabase_db_TP_IPv4") 
        engine = create_engine(
            f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'
        )
        return engine

    # Create SQLAlchemy model for rental_rates
    Base = declarative_base()

    class ReportMetrics(Base):
        __tablename__ = 'report_metrics'
        
        report_uuid = Column(String, primary_key=True)
        report_name = Column(String)
        debt_balance = Column(String)
        net_income = Column(String)
        funds_from_operations = Column(String)
        adjusted_funds_from_operations = Column(String)
        net_asset_value = Column(String)
        ebitda = Column(String)
        same_property_noi = Column(String)
        investment_in_acquisitions = Column(String)
        investment_in_development = Column(String)
        
        def __repr__(self):
            return f"<ReportMetrics(id={self.report_uuid}, report_name={self.report_name})>"


    openAI_prompt_1 = """
        You are an AI model specialized in financial data extraction and analysis. Your task is to analyze financial statements (quarterly and annual reports) and extract key financial metrics with the highest possible accuracy. Follow these instructions carefully:

        1. General Instructions:
        - Extract only numerical values directly stated in the financial statements.
        - If a table is in a certain format (e.g., thousands or millions), extract the values as the actual numerical amounts.
        - If multiple periods are included in a single report (e.g., Q4 results along with full-year data), use quarterly values.
        - If a value is not explicitly stated, do not attempt to infer it, return “Not Available.”
        - Extract dollar values as their actual numerical amounts adjusted to be in the correct scale. 
        - Make sure you understand at which scale the tables are presenting numbers.
        - The output should be structured as JSON.
        - Only output the json content, no other messages are needed.

        2. Key Metrics to Extract:
        Read the attached document and extract the following financial metrics:
    """

    openAI_prompt_2 = """\nYour output should be in structured JSON format as follows: """


    openAI_prompt_3 = """\n
        3. Additional Instructions:
        If a value is unavailable, return "Not Available" as the value instead of an estimated value.
        Do not ever calculate or infer values. Only extract what is directly provided in the financial statements.

        Additional Notes:
        - If multiple periods are included in a single report (e.g., Q4 results along with full-year data), use quarterly values.
        - Prioritize figures from tables, financial summaries, or management discussion sections where applicable.
    """

    # Add OpenAI API key
    load_dotenv(dotenv_path= config_dir / ".env")
    OpenAI.api_key = os.getenv("OPENAI_API_KEY")
    
    assistant_instructions = "You are an expert commercial real estate market analyst. Use your understanding of real estate to retrieve the information I need from the provided financial report."
    
    client = OpenAI()
    
    # Create an assistant with the specified instructions and model
    assistant = client.beta.assistants.create(
        name="Real Estate Data Extractor",
        instructions=assistant_instructions,
        model="gpt-4o-mini-2024-07-18",
        tools=[{"type": "file_search"}]  # Enables file handling
    )

    # Create a vector store. This will contain embeddings of the document chunks once they are attached in the prompt.
    vector_store = client.vector_stores.create(name="CRE Financial Reports")

    assistant = client.beta.assistants.update(
        assistant_id=assistant.id,
        tool_resources={"file_search": {"vector_store_ids": [vector_store.id]}},
    )

    metrics_dict = {
        "debt_balance": {
            "key_terms": ["debt", "liabilities"],
            "metric_name": "Debt Balance",
            "metric_format": "number",
            "prompt_description": "Total debt obligations of the company."
        },
        "net_income": {
            "key_terms": ["net income", "net operating income", "noi"],
            "metric_name": "Net Income (or loss)",
            "metric_format": "number",
            "prompt_description": "The company’s total profit or loss after all expenses."
        },
        "funds_from_operations": {
            "key_terms": ["funds from operations", "ffo"],
            "metric_name": "Funds From Operations (FFO)",
            "metric_format": "number",
            "prompt_description": "Net income adjusted for non-cash items."
        },
        "adjusted_funds_from_operations": { 
            "key_terms": ["adjusted funds", "affo"],
            "metric_name": "Adjusted Funds From Operations (AFFO)",
            "metric_format": "number",
            "prompt_description": "FFO further adjusted for capital expenditures and other relevant items."
        },
        "net_asset_value": {
            "key_terms": ["net asset value", "nav"],
            "metric_name": "Net Asset Value (NAV)",
            "metric_format": "number",
            "prompt_description": "The total value of assets minus liabilities."
        },
        "ebitda": {
            "key_terms": ["ebitda"],
            "metric_name": "EBITDA",
            "metric_format": "number",
            "prompt_description": "Earnings Before Interest, Taxes, Depreciation, and Amortization."
        },
        "occupancy_rate": {
            "key_terms": ["occupancy"],
            "metric_name": "Occupancy",
            "metric_format": "percentage",
            "prompt_description": "The percentage of rentable space currently leased compared to total available space."
        },
        "same_property_noi": {
            "key_terms": ["same property", "same asset"],
            "metric_name": "Same Property NOI",
            "metric_format": "number",
            "prompt_description": "The operating income from properties held in both the current and prior periods, excluding new acquisitions or developments."
        },
        "investment_in_acquisitions": {
            "key_terms": ["acquisitions"],
            "metric_name": "Investment in Acquisitions",
            "metric_format": "number",
            "prompt_description": "Capital spent on purchasing new properties."
        },
        "investment_in_development": {
            "key_terms": ["development"],
            "metric_name": "Investment in Development",
            "metric_format": "number",
            "prompt_description": "Capital allocated for constructing or improving assets."
        }
    }

    def download_pdf(url):
        response = requests.get(url)
        if response.status_code == 200:
            return response.content

    def create_pdf_subsets_for_metrics(pdf_bytes):                
        # Open the source PDF from bytes
        source_pdf = fitz.open(stream=pdf_bytes, filetype="pdf")
        
        # Initialize dictionaries to store page numbers and PDF bytes
        category_pages = {category: [] for category in metrics_dict.keys()}
        
        # Process each page
        for page_num in range(len(source_pdf)):
            page = source_pdf[page_num]
            
            # Extract text from the page
            text = page.get_text().lower()
            
            # Check each category for matches on the page
            for category, meta_dict in metrics_dict.items():
                if any(indicator.lower() in text for indicator in meta_dict["key_terms"]):
                    # Add page number to this category
                    category_pages[category].append(page_num)
        
        # Remove empty categories
        category_pages = {k: v for k, v in category_pages.items() if v}

        pdf_sections_byte_objects = {}
        # Create a PDF byte object for all pages that contain the relevant category/metric
        for category, page_numbers in category_pages.items():
            # Create a new PDF for this category
            category_pdf = fitz.open()
            
            # Add all relevant pages to this category's PDF
            for page_num in sorted(set(page_numbers)):  # Using set to remove duplicates
                category_pdf.insert_pdf(source_pdf, from_page=page_num, to_page=page_num)
            
            # Save the category PDF to a bytes object
            output_stream = io.BytesIO()
            category_pdf.save(output_stream)
            output_stream.seek(0)  # Reset stream position to the beginning
            
            # Store the PDF bytes
            pdf_sections_byte_objects[category] = output_stream.getvalue()
            
            # Close the category PDF
            category_pdf.close()
        
        # Close the source PDF
        source_pdf.close()
        
        # Convert to 1-based page numbers in the result dictionary for human-readable output
        result = {category: [page_num + 1 for page_num in sorted(set(pages))] 
                for category, pages in category_pages.items()}
    
        return result, pdf_sections_byte_objects


    def generate_prompts(pdf_dict):
        metrics_extraction_prompts = {} 

        for metric, pages in pdf_dict.items():
            # create prompt for this metric
            prompt = openAI_prompt_1 + "\n" + f"{metrics_dict[metric]['metric_name']}: " + metrics_dict[metric]['prompt_description'] + "\n" + \
                     openAI_prompt_2 + "\n\n{" + f"{metric}: {metrics_dict[metric]['metric_format']}" + "}" + openAI_prompt_3
                     
            metrics_extraction_prompts[metric] = prompt

        missing_metrics = [metric for metric in metrics_dict.keys() if metric not in pdf_dict.keys()]

        return metrics_extraction_prompts, missing_metrics


    def extract_metric_from_report(client, assistant, pdf_bytes, metric, prompt):
        
        file_obj = io.BytesIO(pdf_bytes)
        file_obj.name = f"{metric}.pdf"

        message_file = client.files.create(
                file=file_obj, 
                purpose="assistants"
            )

        # Create a new thread to eliminate information sharing between different brochures
        thread = client.beta.threads.create()

        thread = client.beta.threads.create(
            messages=[
                {
                    "role": "user",
                    "content": prompt,
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

        try:
            # Attempt to parse the JSON content
            message_dict = json.loads(message_content)
        except:    
            message_dict = json.loads(f'{{"{metric}": "Not Available"}}')

        metric_value = message_dict[metric]

        client.files.delete(message_file.id)
        
        return metric_value


    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)
    
    print(f"Looking for PDFs in gs://{gcs_bucket}/{gcs_input_path}")
    
    # List all blobs in the input path
    blobs = list(bucket.list_blobs(prefix=gcs_input_path))
    pdf_blobs = [blob for blob in blobs if blob.name.endswith('.pdf')]
    
    print(f"Found {len(pdf_blobs)} PDF files")
    
    # Create a list of tuples containing (filename, file_bytes)
    pdf_files = []
    
    for blob in pdf_blobs:
        # Extract the filename without the path
        filename = Path(blob.name).name
        file_bytes = blob.download_as_bytes()
        file_obj = io.BytesIO(file_bytes)
        file_obj.name = filename

        pdf_files.append((filename, file_obj))



    processed_report_list = []
    for report_name, pdf_bytes in pdf_files:
        pdfs_created, pdf_byte_objects = create_pdf_subsets_for_metrics(pdf_bytes)
        extraction_prompts, missing_metrics = generate_prompts(pdfs_created)
        uuid = uuid.uuid4().hex

        # Initialize with missing metrics as "Not Available"
        report_extracted_output = {metric: "Not Available" for metric in missing_metrics }
        
        print(f"Extracting from {report_name}: \n{list(extraction_prompts.keys())}\n\n")
        for metric_name in extraction_prompts.keys():
            if metric_name in pdf_byte_objects:
                prompt = extraction_prompts[metric_name]
                byte_object = pdf_byte_objects[metric_name]

                # metric_pdf_subset_file = report_dir / f"{metric_name}.pdf"
                metric_value = extract_metric_from_report(client, assistant, byte_object, metric_name, prompt)
                report_extracted_output[metric_name] = metric_value

        processed_report_list.append({"report_name": report_name, "report_uuid": uuid, **report_extracted_output})
    
    
    ################################################
    ########  Save Rental Rates to the DB  #########
    ################################################
    
    try:
        # Create database engine
        engine = get_db_engine()
        # Create tables if they don't exist
        Base.metadata.create_all(engine)
        
        # Create a session to interact with the database
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Convert the data to a list of ReportMetrics objects
        db_reports = []
        for rep in processed_report_list:
            db_reports.append(
                ReportMetrics(
                    report_uuid=rep.get('report_uuid', ''),
                    report_name=rep.get('report_name', ''),
                    debt_balance=rep.get('debt_balance', ''),
                    net_income=rep.get('net_income', ''),
                    funds_from_operations=rep.get('funds_from_operations', ''),
                    adjusted_funds_from_operations=rep.get('adjusted_funds_from_operations', ''),
                    net_asset_value=rep.get('net_asset_value', ''),
                    ebitda=rep.get('ebitda', ''),
                    same_property_noi=rep.get('same_property_noi', ''),
                    investment_in_acquisitions=rep.get('investment_in_acquisitions', ''),
                    investment_in_development=rep.get('investment_in_development', ''),
                )
            )
        
        # Add all rental rates to the database
        session.add_all(db_reports)
        session.commit()
        print(f"Successfully saved {len(db_reports)} report metrics entries to the database")
    
    except Exception as e:
        if 'session' in locals():
            session.rollback()
        print(f"Error saving to database: {e}")
    
    finally:
        if 'session' in locals():
            session.close()
    