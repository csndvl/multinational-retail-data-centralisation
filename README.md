# Multinational Retail Data Centralisation

This was a scenario based project set by Ai Core forming part of the data engineering bootcamp. This scenario aimed to build skills in data extraction and cleaning from multiple sources in python before uploading the date to a local postgres database.

Scenario: You work for a multinational company that sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location.

Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. You will then query the database to get up-to-date metrics for the business


## Project Utils
  1. Data extraction. In "data_extraction.py" we store methods responsible for retrieving data from different sources         into pandas data frame.
  2. Data cleaning. In "data_cleaning.py" we develop the class DataCleaning that clean different tables, which we 
     retrived from "data_extraction.py".
  3. Uploading data into the database. We write DatabaseConnector class "database_utils.py", which initiates the       
     database engine based on credentials provided in ".yml" file.


## Milestone 2 - Data Extraction and Data Cleaning
Goals: To extract all the data from the multitude of data sources, clean it, and then store it in a local database.
  
  - Data are extracted from multiples sources:
    1. RDS Tables
        - Order Table - (extracted by creating a connection with AWS RDS using its own credentials)
        - Legacy User - (extracted by creating a connection with AWS RDS using its own credentials)
    2. PDFs
        - Card Details - (extracted with the use of tabula library to read pdf files)
    3. APIs
        - Store Detials - (extracted using API Get Request with their specific endpoints)
    4. AWS S3 Buckets
        - Product Details - (extracted using AWS s3 address to get bucket name and file name)
        - Order Time Data - (extracted using AWS s3 address to get bucket name and file name)
            
  - Data are cleaned using Pandas:
    1. Order Table
       - Unwanted columns are dropped
         ```
         df.drop([column_name], axis = 1, inplace = True)
         ```
    2. Legacy User
       - Changed "NULL" strings into NULL values
       - Converted join_date column into a datetime data type
       - Removed NULL values
         ```
         df = df.replace("NULL", np.NaN)
         df['join_date'] = pd.to_datetime(df['join_date'], errors = 'coerce')
         df = df.dropna()
         ```
    3. Card Details
       - Changed "NULL" strings into NULL values
       - Removed duplicate card numbers
       - Removed card numbers with letters
       - Converted date_payment_confirmed column into a datetime data type
       - Removed NULL values
         ```
         df = df.replace("NULL", np.NaN)
         df = df.drop_duplicates(subset = ['card_number'])
         df = df[~df['card_number'].str.contains(r'[a-zA-Z]')]
         df["date_payment_confirmed"] = pd.to_datetime(df["date_payment_confirmed"], errors = 'coerce')
         df = df.dropna()
         ```
   
  
    


## Milestore 3 - Creating Database Schema
Goals: Develop the star-based schema of the database, ensuring that the columns are of the correct data types.
