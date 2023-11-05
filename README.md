# Multinational Retail Data Centralisation

This was a scenario based project set by Ai Core forming part of the data engineering bootcamp. This scenario aimed to build skills in data extraction and cleaning from multiple sources in python before uploading the date to a local postgres database.

Scenario: You work for a multinational company that sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location.

Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. You will then query the database to get up-to-date metrics for the business


## Project Utils
  1. Data extraction. In "data_extraction.py", we store methods responsible for retrieving data from different sources into pandas data frame.
  2. Data cleaning. In "data_cleaning.py", we develop the class DataCleaning that clean different tables, which we retrived from "data_extraction.py".
  3. Uploading data into the database. We write DatabaseConnector class "database_utils.py", which initiates the database engine based on credentials provided in ".yml" file.
  4. Database wrangling is performed inside "database_wrangling.sql". This is where all the columns are converted into its correct data types, all the dim tables are given primary key, and also where foreign keys are added into order table
  5. Data queries are performed inside "scenario_queries.sql"


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
    4. Store Details
       - Changed "NULL" strings into NULL values
       - Removed non-digit in staff_number columns
       - Converted opening_date column into a datetime data type
       - Removed NULL values
         ```
         df = df.replace("NULL", np.NaN)
         df['staff_numbers'] = df['staff_numbers'].str.replace(r"(\D)", "", regex = True)
         df['opening_date'] = pd.to_datetime(df['opening_date'], errors = 'coerce')
         df = df.dropna()
         ```
    5. Product Details
       - Changed "NULL" strings into NULL values
       - Turned all weight into kg units
       - Removed NULL values
         ```
         df = df.replace("NULL", np.NaN)
         new_weight = []
         for weight in df['weight']:
            correct_weight = self.convert_product_weights(weight)
            new_weight.append(correct_weight)
         df['weight'] = new_weight
         df = df.dropna()
         ```
    6. Order Time Data
       - Changed columns into numeric data type
       - Dropped NULL values in month, year, and day column
         ```
         df['month'] = pd.to_numeric(df['month'], errors = 'coerce')
         df['year'] = pd.to_numeric(df['year'], errors = 'coerce')
         df['day'] = pd.to_numeric(df['day'], errors = 'coerce')
         df = df.dropna(subset = ['month', 'year', 'day'])
         ```
         
  - Data are uploaded in local postgres database
    1. Made a connection using local credentials
    2. Uploaded each clean data into their perspective table name
       - Order Table > order_table
       - Legacy User > dim_user
       - Card Details > dim_card_details
       - Store Details > dim_store_details
       - Product Details > dim_product_details
       - Order Time Data > dim_date_times


## Milestore 3 - Creating Database Schema
Goals: Develop the star-based schema of the database, ensuring that the columns are of the correct data types.
- Tables were updated to ensure that data were stored in the correct data types. To dedtermine the maximum number of characters for the VARCHAR(?) data type, a query was performed, before the output was used in the VARCHAR data type.
  ```
    -- Find the longest card number length
  SELECT MAX(LENGTH(card_number::TEXT)) FROM order_table
  SET LIMIT 1; --19
  
  -- Find the longest store code length
  SELECT MAX(LENGTH(store_code::TEXT)) FROM order_table
  SET LIMIT 1; --12
  
  -- Find the longest product code length
  SELECT MAX(LENGTH(product_code::TEXT)) FROM order_table
  SET LIMIT 1; --11
  
  -- Alter column data types
  ALTER TABLE order_table
  ALTER COLUMN date_uuid TYPE UUID
  USING date_uuid::uuid,
  ALTER COLUMN user_uuid TYPE UUID
  USING user_uuid::uuid,
  ALTER COLUMN card_number TYPE VARCHAR(19),
  ALTER COLUMN store_code TYPE VARCHAR(12),
  ALTER COLUMN product_code TYPE VARCHAR(11),
  ALTER COLUMN product_quantity TYPE SMALLINT;
  ```
- The use of case statement was introduced to improve readability in dim_product table
  ```
  UPDATE dim_product
  SET weight_class = CASE
  WHEN weight < 2 then 'Light'
  WHEN weight >= 2 AND weight < 40 then 'Mid_Sized'
  WHEN weight >= 40 AND weight < 140 then 'Heavy'
  WHEN weight >= 140 then 'Truck_Required'
  ELSE NULL
  END;
  ```
- Primary keys are added in the dim tables
  ```
  ALTER TABLE dim_date_times
  ADD PRIMARY KEY (date_uuid);
  
  ALTER TABLE dim_user
  ADD PRIMARY KEY (user_uuid);
  
  ALTER TABLE dim_card_details
  ADD PRIMARY KEY (card_number);
  
  ALTER TABLE dim_store_details
  ADD PRIMARY KEY (store_code);
  
  ALTER TABLE dim_product
  ADD PRIMARY KEY (product_code);
  ```
- Foreign keys are added to link tables
  ```
  ALTER TABLE order_table
  ADD FOREIGN KEY (date_uuid)
  REFERENCES dim_date_times (date_uuid);
  
  ALTER TABLE order_table
  ADD FOREIGN KEY (user_uuid)
  REFERENCES dim_user (user_uuid); 
  
  ALTER TABLE order_table
  ADD FOREIGN KEY (store_code)
  REFERENCES dim_store_details (store_code);
  
  ALTER TABLE order_table
  ADD FOREIGN KEY (product_code)
  REFERENCES dim_product (product_code);
  
  ALTER TABLE order_table
  ADD FOREIGN KEY (card_number)
  REFERENCES dim_card_details (card_number)
  ```
- Data cleaning was required to ensure the foreign and primary keys matched. The code below was used to see the difference between order_table and dim_user table
  ```
  SELECT distinct(order_table.user_uuid)
  FROM order_table
  LEFT JOIN dim_user
  ON order_table.user_uuid = dim_user.user_uuid
  WHERE dim_user.user_uuid IS NULL'''
  ```
  

## Milestone 4 - Querying the data for different scenario
Goal - Querying and extracting data from the local database to get some up-to-date metrics, having a more data-driven decisions and get better understanding of its sales.
1. How many stores do the business have and in which countries?
   ```
   SELECT country_code, COUNT(store_code) as total_no_stores
   FROM dim_store_details
   GROUP BY country_code 
   ORDER BY total_no_stores DESC;
   ```
2. Which locations have the most stores?
   ```
   SELECT locality, COUNT(store_code) as total_no_stores
   FROM dim_store_details
   GROUP BY locality
   ORDER BY total_no_stores DESC
   LIMIT 7;
   ```
3. Which months produce the most sales overall time of records?
   ```
   SELECT ROUND(SUM(dim_product.product_price * order_table.product_quantity)::NUMERIC,2) as total_sales, dim_date_times.month 
   FROM dim_product
   JOIN order_table
   ON order_table.product_code = dim_product.product_code
   JOIN dim_date_times
   ON dim_date_times.date_uuid = order_table.date_uuid
   GROUP BY dim_date_times.month
   ORDER BY total_sales DESC;
   ```
4. How many sales come online?
   ```
   SELECT COUNT(dim_product.product_code) as number_of_sales, SUM(order_table.product_quantity) as product_quantity_count, 
   CASE
   WHEN dim_store_details.store_type IN ('Super Store', 'Local', 'Outlet', 'Mall Kiosk') THEN 'Offline'
   ELSE 'Web'
   END as location
   FROM dim_product
   JOIN order_table
   ON order_table.product_code = dim_product.product_code
   JOIN dim_store_details
   ON dim_store_details.store_code = order_table.store_code
   GROUP BY location 
   ORDER BY number_of_sales ASC;
   ```
5. What percentage of sales come through each type of store?
   ```
   SELECT dim_store_details.store_type, ROUND(SUM(dim_product.product_price * order_table.product_quantity)::NUMERIC,2) as total_sales, 
   ROUND(SUM(100 * dim_product.product_price * order_table.product_quantity)::NUMERIC/ SUM(SUM(dim_product.product_price * order_table.product_quantity)::NUMERIC) OVER (), 2) as "percentage_total(%)"
   FROM dim_product
   JOIN order_table
   ON order_table.product_code = dim_product.product_code
   JOIN dim_store_details
   ON dim_store_details.store_code = order_table.store_code
   GROUP BY dim_store_details.store_type
   ORDER BY "percentage_total(%)" DESC;
   ```
6. Which month in the year produced the most sales?
   ```
   SELECT ROUND(SUM(dim_product.product_price * order_table.product_quantity)::NUMERIC ,2) as total_sales,
   dim_date_times.year, dim_date_times.month
   FROM dim_product
   JOIN order_table
   ON order_table.product_code = dim_product.product_code
   JOIN dim_date_times
   ON dim_date_times.date_uuid = order_table.date_uuid
   GROUP BY dim_date_times.year, dim_date_times.month
   ORDER BY total_sales DESC;
   ```
7. What is the staff count?
   ```
   SELECT SUM(staff_numbers) as total_staff_numbers, country_code
   FROM dim_store_details
   GROUP BY country_code
   ORDER BY total_staff_numbers DESC;
   ```
8. Which German store saling the most?
   ```
   SELECT ROUND(SUM(dim_product.product_price * order_table.product_quantity)::NUMERIC ,2) as total_sales,
   dim_store_details.store_type, dim_store_details.country_code
   FROM dim_product
   JOIN order_table
   ON order_table.product_code = dim_product.product_code
   JOIN dim_store_details
   ON dim_store_details.store_code = order_table.store_code
   WHERE country_code = 'DE'
   GROUP BY dim_store_details.store_type, dim_store_details.country_code
   ORDER BY total_sales;
   ```
