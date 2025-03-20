def get_REIT_report_data():
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

    config_dir = Path("/opt/airflow/config")
    reports_dir = Path("/opt/airflow/data/financial_reports/available_reports.json")
    output_bucket = Path("/opt/airflow/data/financial_reports")

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

    load_dotenv(dotenv_path= config_dir / ".env")
    OpenAI.api_key = os.getenv("OPENAI_API_KEY")
    
    assistant_instructions = "You are an expert commercial real estate market analyst. Use your understanding of real estate to retrieve the information I need from the provided financial report."
    
    client = OpenAI()
    
    assistant = client.beta.assistants.create(
        name="Real Estate Data Extractor",
        instructions=assistant_instructions,
        model="gpt-4o-mini-2024-07-18",
        tools=[{"type": "file_search"}]  # Enables file handling
    )

    # Create a vector store. This will contain embeddings of the document chunks once they are attached in the prompt.
    vector_store = client.beta.vector_stores.create(name="CRE Financial Reports")

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
        "EBITDA": {
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
        "same_property_NOI": {
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

    def create_pdf_subsets_for_metrics(input_pdf_path, output_dir):        
        # Open the source PDF
        source_pdf = fitz.open(input_pdf_path)
        category_pages = {category: [] for category in metrics_dict.keys()}

        output_pdfs = {}
        
        # Process each page
        for page_num in range(len(source_pdf)):
            page = source_pdf[page_num]
            
            # Extract text from the page
            text = page.get_text().lower()

            # Check each category for matches
            for category, meta_dict in metrics_dict.items():
                if any(indicator.lower() in text for indicator in meta_dict["key_terms"]):
                    # Add page number to this category
                    category_pages[category].append(page_num)
        
        # Remove empty categories
        category_pages = {k: v for k, v in category_pages.items() if v}
        
        # Create a PDF for each category that has at least one page
        for category, page_numbers in category_pages.items():
                
            # Create a new PDF for this category
            output_pdf_path = os.path.join(output_dir, f"{category}.pdf")
            category_pdf = fitz.open()
            
            # Add all relevant pages to this category's PDF
            for page_num in sorted(set(page_numbers)):  # Using set to remove duplicates
                category_pdf.insert_pdf(source_pdf, from_page=page_num, to_page=page_num)
            
            # Save the category PDF
            category_pdf.save(output_pdf_path)
            category_pdf.close()
            
            # Convert to 1-based page numbers for reporting
            human_readable_pages = [page_num + 1 for page_num in sorted(set(page_numbers))]
        
        # Close the source PDF
        source_pdf.close()
        
        # Convert to 1-based page numbers in the result dictionary
        result = {category: [page_num + 1 for page_num in pages] 
                for category, pages in category_pages.items()}

        return result


    def generate_prompts(pdf_dict):
        metrics_extraction_prompts = {} 

        for metric, pages in pdf_dict.items():
            # create prompt for this metric
            prompt = openAI_prompt_1 + "\n" + f"{metrics_dict[metric]['metric_name']}: " + metrics_dict[metric]['prompt_description'] + "\n" + \
                     openAI_prompt_2 + "\n\n{" + f"{metric}: {metrics_dict[metric]['metric_format']}" + "}" + openAI_prompt_3
                     
            metrics_extraction_prompts[metric] = prompt

        missing_metrics = [metric for metric in metrics_dict.keys() if metric not in pdf_dict.keys()]

        return metrics_extraction_prompts, missing_metrics


    def extract_metric_from_report(client, assistant, metric_pdf_subset_file, metric, prompt):
        message_file = client.files.create(
                file=open(metric_pdf_subset_file, "rb"), 
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
        message_dict = json.loads(message_content)
        metric_value = message_dict[metric]

        client.files.delete(message_file.id)
        
        return metric_value


    filtered_output_dir = output_bucket / "filtered_pdfs"

    with open(reports_dir, 'r') as f:
        all_reports_df = pd.DataFrame(json.load(f))

    processed_report_list = []
    for idx, row in all_reports_df.iterrows():
        report_url = row['report_url']
        report_name = f"{row['report_firm']}_{row['report_period']}"
        temp_file_name = f"{row['report_firm']}_report_temp_file.pdf"
        report_dir = filtered_output_dir / f"{row['report_firm']}"
        os.makedirs(report_dir, exist_ok=True)
        
        if report_url:
            # Download the PDF
            report_pdf = download_pdf(report_url)

            with open(temp_file_name, "wb") as f:
                f.write(report_pdf)
            
            pdfs_created = create_pdf_subsets_for_metrics(temp_file_name, report_dir)
            extraction_prompts, missing_metrics = generate_prompts(pdfs_created)

            # Initialize with missing metrics as "Not Available"
            report_extracted_output = {metric: "Not Available" for metric in missing_metrics }
            
            print(f"Extracting from {report_name}: \n{list(extraction_prompts.keys())}\n\n")
            for metric_name in extraction_prompts.keys():
                prompt = extraction_prompts[metric_name]

                metric_pdf_subset_file = report_dir / f"{metric_name}.pdf"
                metric_value = extract_metric_from_report(client, assistant, metric_pdf_subset_file, metric_name, prompt)
                report_extracted_output[metric_name] = metric_value

            processed_report_list.append({"report_name": report_name, "metrics": report_extracted_output})
    
    # Save the updated data to the JSON file
    with open(output_bucket / "processed_reports.json", 'w') as f:
        json.dump(processed_report_list, f, indent=2)