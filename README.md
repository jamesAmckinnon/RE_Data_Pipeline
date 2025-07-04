# Real Estate Market Analysis Data Pipeline
The focus of this project was to build a sophisticated data engineering pipeline that automates the process of extracting, transforming, enriching and storing real estate related data that will be used in a web application to support real estate investment decisions. 

## Project Description
Various tools and technologies were used to create a backend system capable of web scraping, information extraction and data enrichment. Apache Airflow DAGs were created for extracting and processing commercial property listings, rental rates, property brochures, and financial reports. The completed system is up and running on Google Cloud Platform and will continue to collect data on a scheduled basis. The processed data is stored in a Supabase database.

### 🤖 Technology Stack
- Apache Airflow
- GCP Compute Engine VM and Storage
- OpenAI Assistants API
- Docker for Local Testing
- Supabase Database
- SQLAlchemy
- Python


## ⚛︎ Airflow DAGs
![Airflow Screenshot](https://github.com/jamesAmckinnon/RE_Data_Pipeline/blob/master/images/Airflow_Get_Listings.png)


### Property Listings DAG
Extracts and processes property listings from commercial brokerage websites. The websites are scraped using the publicly exposed API endpoints that populate property listings pages.

For each property, additional information is added before a record is created in the datatbase. This includes:
- Information from the property's brochure
- Zoning information based on the property's coordinates
- The businesses and amenities within a radius around the property

![Airflow Screenshot](https://github.com/jamesAmckinnon/RE_Data_Pipeline/blob/master/images/Get_Listings_DAG.png)


### Rental Rates DAG
Extracts and processes rental rates from rental listing websites. These rental rates are stored as individual rates as well as aggregated rates. 

For aggregated rates, a spatial grid is generated over a pre-defined area, such as a city, and then an average of the rental rates from properties located within each grid cell is calcualted.

![Airflow Screenshot](https://github.com/jamesAmckinnon/RE_Data_Pipeline/blob/master/images/Rental_Rates_DAG.png)


### Financial Report Processing DAG
Extracts financial metrics from REIT financial report PDFs. 

Each PDF is downloaded and parsed page-by-page to identify which pages contain specific financial metrics. The relevant pages for each metric are combined into a separate PDF. For each of these metric-specific PDFs, a custom prompt related to the metric is generated and this prompt, along with the PDF, is submitted to an OpenAI assistant which extracts the metric.

![Airflow Screenshot](https://github.com/jamesAmckinnon/RE_Data_Pipeline/blob/master/images/Financial_Reports_DAG.png)
