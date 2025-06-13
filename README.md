# Real Estate Market Analysis Data Pipeline
The focus of this project was to build a sophisticated data engineering pipeline that automates the process of extracting, transforming, enriching and storing real estate related data that will be used in a web application to support real estate investment decisions. 

## Project Description
Various tools and technologies were used to create a backend system capable of handling workflows that involve web scraping, information extraction and data enrichment. The completed system is up and running on Google Cloud Platform and will continue to collect data on a scheduled basis. Apache Airflow DAGs were created for extracting and processing commercial property listings, rental rates, property brochures, and financial reports. The processed data is stored in a Supabase database.

### 🤖 Technology Stack
- Apache Airflow
- GCP Compute Engine VM and Storage
- OpenAI Assistants API
- Docker for Local Testing
- Supabase Database
- SQLAlchemy


## ⚛︎ Airflow DAGs
![Airflow Screenshot](https://github.com/jamesAmckinnon/RE_Data_Pipeline/blob/master/images/Airflow_Get_Listings.png)


### Property Listings DAG
Extracts and processes property listings from commercial brokerage websites. The websites are scraped using publicly exposed API endpoints that are used to populate their listings pages.

For each property, additional information is added before a record is created in the datatbase. This includes:
- Information from the property's brochure
- Location-specific zoning information
- The businesses and amenities within a radius

![Airflow Screenshot](https://github.com/jamesAmckinnon/RE_Data_Pipeline/blob/master/images/Get_Listings_DAG.png)


### Rental Rates DAG
Extracts and processes rental rates from rental listing websites. These rental rates are stored as individual rates as well as aggregated rates. A spatial grid is generated in a pre-defined area, such as a city, and then an average of the rental rates from properties that have coordinates that fall within each grid cell is calcualted.

![Airflow Screenshot](https://github.com/jamesAmckinnon/RE_Data_Pipeline/blob/master/images/Rental_Rates_DAG.png)


### Financial Report Processing DAG
Extracts financial metrics from REIT financial report PDFs. Each PDF is downloaded and parsed page-by-page to identify which pages contain key financial terms. The relevant pages for each metric are combined into a separate PDF. For each of these metric-specific PDFs, a custom prompt related to the associated metric is generated and submitted to an OpenAI assistant.

![Airflow Screenshot](https://github.com/jamesAmckinnon/RE_Data_Pipeline/blob/master/images/Financial_Reports_DAG.png)
