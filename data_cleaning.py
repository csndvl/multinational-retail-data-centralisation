from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd
import numpy as np


class DataCleaning():
    ''''''

    def clean_user_data(self, df):
        '''Cleans the legacy_user table'''

        # Remove null values
        df = df.replace("NULL", np.NaN)
        df = df.dropna() 
        df = df.drop_duplicates(subset = ['phone_number', 'email_address']) # ASK ABOUT THE SUBSET

        # First_name and Last_name cleaning
        df['first_name'] = df['first_name'].str.title()
        df['last_name'] = df['last_name'].str.title()
        # Can add special character cleaning

        # Converts date_of_birth and join_date column into a datetime data type and any invalid data into Not a Time (NaT)
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors = 'coerce') 
        df['join_date'] = pd.to_datetime(df['join_date'], errors = 'coerce') 
        df = df.dropna(subset = ['date_of_birth', 'join_date']) # Drops all null values in date_of_birth and join_date column

        # Company column cleaning
        df['company'] = df['company'].str.title()

        # Email column cleaning
        

        # Address column cleaning
        df['address'] = df['address'].str.title()
        df['address'] = df['address'].str.replace(".", "", regex = False) # Ask whether the address format can be change?

        # Country column cleaning

        # Country_code cleaning
        df['country_code'] = df['country_code'].str.replace('GGB', 'GB')

        # Phone number cleaning 
        df['phone_number'] = df['phone_number'].str.replace(r"(\D)", "", regex = True) # Removes all non-digit
        df['phone_number'] = df['phone_number'].str.replace(" ", "") # Removes all white space
        #MORE

        # Remove index column and replace with a new one
        df.reset_index(inplace = True)
        df.drop(df.columns[0], axis=1, inplace=True) 
        df['index'] = range(1, len(df) + 1)

        print("user cleaning done\n")
        return (df) 
    

    def clean_card_data(self, df):
        '''Cleans card details'''

        # Remove null values. started with 15309
        df = df.replace("NULL", np.NaN)
        df = df.dropna()
        df = df.drop_duplicates(subset = ['card_number'])

        # Cleans card number
        df['card_number'] = df['card_number'].apply(str)
        df['card_number'] = df['card_number'].str.replace('?','')
        
        #Converts expiry_date and date_payment_confirmed column into a datetime data type
        df["date_payment_confirmed"] = pd.to_datetime(df["date_payment_confirmed"], errors = 'coerce')
        #df["expiry_date"] = pd.to_datetime(df["expiry_date"], format = '%m%Y' ,errors = 'coerce')
        df = df.dropna(subset = ['date_payment_confirmed']) 

        print ("card cleaning done\n")
        return df
    
    def clean_store_details(self, df):
        pass






if __name__ == "__main__":
    
    db_con = DatabaseConnector()
    data_ex = DataExtractor()
    data_clean = DataCleaning()

    number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    store_details_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
    api_key = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

    # Database credentials and Engine connection
    aws_creds = db_con.read_db_creds()
    engine = db_con.init_db_engine(aws_creds)
    local_creds = db_con.local_creds()

    # Retrieves and clean data
    user_data = data_ex.read_rds_table(engine, "legacy_users")
    clean_user_data = data_clean.clean_user_data(user_data)
    number_of_stores = data_ex.list_number_of_stores(number_of_stores_endpoint, api_key)
    store_details =  data_ex.retrieve_stores_data(store_details_endpoint, number_of_stores, api_key)

    card_details = data_ex.retrieve_pdf_data()
    clean_card_details = data_clean.clean_card_data(card_details)

    # Upload to local database
    db_con.upload_to_db(clean_user_data, 'dim_user', local_creds) # Upload user data
    db_con.upload_to_db(clean_card_details, 'dim_card_details', local_creds) # Upload card details

   

