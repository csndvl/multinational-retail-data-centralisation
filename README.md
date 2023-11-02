# Multinational Retail Data Centralisation

This was a scenario based project set by Ai Core forming part of the data engineering bootcamp. This scenario aimed to build skills in data extraction and cleaning from multiple sources in python before uploading the date to a local postgres database.

Scenario: You work for a multinational company that sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location.

Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. You will then query the database to get up-to-date metrics for the business

# Milestone 2 - Data Extraction and Data Cleaning
  - Data were extracted from multiples sources (RDS Tables, PDFs, APIs, and AWS S3 Buckets)

# Project Utils
  1. Data extraction. In "data_extraction.py" we store methods responsible for retrieving data from different sources         into pandas data frame.
  2. Data cleaning. In "data_cleaning.py" we develop the class DataCleaning that clean different tables, which we 
     retrived from "data_extraction.py".
  3. Uploading data into the database. We write DatabaseConnector class "database_utils.py", which initiates the       
     database engine based on credentials provided in ".yml" file. 
