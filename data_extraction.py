from database_utils import DatabaseConnector as db_con
import pandas as pd
import tabula as tb
import requests
import json
import numpy as np


class DataExtractor():
    '''Extracts data from the database.'''     

    def read_rds_table(self, db_con, table_name): 
        """The function reads a table from the RDS database and prints it as a dataframe."""
        print ("running read_rds_table...")
        data = pd.read_sql_table(table_name, db_con)
        df = pd.DataFrame(data)
        print ("done")
        return(df)
    
    def retrieve_pdf_data(self):
        '''The function reads card details inside the pdf file'''
        print ("running retrieve_pdf_data...")
        link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        card_details =  tb.read_pdf(link, pages = "all") # Read pdf file
        card_details = pd.concat(card_details) # Put all multiple dataframe into a single dataframe
        print ("done")
        return (card_details)
    
    def list_number_of_stores(self, endpoint, api_key):
        print("running list_number_of_stores...")
        response = requests.get(endpoint, headers = api_key).content
        result = json.loads(response)
        result = result['number_stores']
        print ("done")
        return (result) # result = 451
    
    def retrieve_stores_data(self, endpoint, num_stores, api_key):
        print("running retrieve_stores_data")
        store_details = []
        for stores in range(0, num_stores):
            response = requests.get(f"{endpoint}{stores}", headers = api_key).json()
            store_details.append(pd.DataFrame(response, index = [np.NaN]))
        store_details_df = pd.concat(store_details)
        print ("done")
        return (store_details_df)
       
      



# db = db_con()
# #creds = db.read_db_creds()
# engine = db.init_db_engine()
test1 = DataExtractor()
#test1.read_rds_table(engine, "legacy_users")
# link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
# test1.retrieve_pdf_data()

number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
store_details_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
api_key = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
test1.retrieve_stores_data(store_details_endpoint, number_of_stores_endpoint, api_key)