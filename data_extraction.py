from database_utils import DatabaseConnector as db_con
import pandas as pd
import tabula as tb
import requests
import json
import numpy as np
import boto3
from io import BytesIO


class DataExtractor():
    '''This function extracts data from different sources'''     

    def read_rds_table(self, db_con, table_name): 
        """The function reads a table from the RDS database and prints it as a dataframe."""
        print ("running read_rds_table")
        data = pd.read_sql_table(table_name, db_con)
        df = pd.DataFrame(data)
        print ("done")
        return (df)
    
    def retrieve_pdf_data(self):
        '''The function reads card details inside the pdf file'''
        print ("running retrieve_pdf_data")
        link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        card_details =  tb.read_pdf(link, pages = "all") # Read pdf file
        card_details = pd.concat(card_details) # Put all multiple dataframe into a single dataframe
        print ("done")
        return (card_details)
    
    def list_number_of_stores(self, endpoint, api_key):
        '''The function counts how many stores in the data'''
        print("running list_number_of_stores")
        response = requests.get(endpoint, headers = api_key).content
        result = json.loads(response) # Loads is used to convert JSON string into python dictionary
        result = result['number_stores']
        print ("done")
        return (result) # result = 451
    
    def retrieve_stores_data(self, endpoint, num_stores, api_key):
        '''The function collects all store data using its store numbers'''
        print("running retrieve_stores_data")
        store_details = []
        for stores in range(0, int(num_stores)): # Iterates from 0 to the total of stores
            response = requests.get(f"{endpoint}{stores}", headers = api_key).json()
            store_details.append(pd.DataFrame(response, index = [np.NaN]))
        store_details_df = pd.concat(store_details) # Put all multiple dataframe inside store_detail into one
        print ("done")
        return (store_details_df)
    
    def extract_from_s3(self, address):
        '''The function extracts data from AWS s3'''
        print ("running extract from s3")
        client = boto3.client('s3') # Creates a s3 client
        if "s3://" in address:
            split_address = address.replace("s3://", "").split("/", 1) # Splits the bucket name and file name

        elif "https://" in address:
            split_address = address.replace("https://", "").split("/", 1) # Splits the bucket name and file name

        bucket_name = 'data-handling-public' # Set bucket name into data-handling-public
        s3key_path = "/".join(split_address[1:])

        data = client.get_object(Bucket = bucket_name, Key = s3key_path) # Retrieves products.csv
        data =  data['Body'].read()
        
        if 'csv' in s3key_path:
            df = pd.read_csv(BytesIO(data)) # Creates an in-memory buffer for the data retrieved from the S3 bucket and can be read by read_csv
        elif 'json' in s3key_path:
            df = pd.read_json(BytesIO(data)) # Creates an in-memory buffer for the data retrieved from the S3 bucket and can be read by read_json
        print ("done")
        return (df)
    