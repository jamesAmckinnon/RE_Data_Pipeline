# Real Estate Market Analysis Data Pipeline
The focus of this project was to build a sophisticated data engineering pipeline that automates the process of extracting, transforming and storing real estate related data that will be used in a web application to support real estate investment decisions. 

## Project Description
Various tools and technologies were used to create a backend system capable of handling workflows that involve web scraping, information extraction and data enrichment. The result is an extensible and scalable pipeline that utilizes modern cloud and data engineering tools. 

The completed system is up and running on Google Cloud Platform and will continue to collect data on a scheduled basis. Apache Airflow DAGs were created for extracting and processing commercial property listings, rental rates, property brochures, and financial reports. The processed data is stored in a Supabase database.

### 🤖 Technology Stack
- Apache Airflow
- GCP Compute Engine VM and Storage
- OpenAI Assistants API
- Docker for Local Testing
- Supabase Database
- SQLAlchemy

###  Airflow DAGs


![Airflow Screenshot](https://github.com/jamesAmckinnon/RE_Data_Pipeline/blob/master/images/Airflow_Get_Listings.png)
