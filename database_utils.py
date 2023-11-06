import yaml
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
import os


class DatabaseConnector():
    '''This function reads databases credentials, initialise engines, list table names, and upload clean data into local database'''

    def __init__(self):
        load_dotenv('.env')
        self.RDS_HOST = os.getenv('RDS_HOST')
        self.RDS_PASSWORD = os.getenv('RDS_PASSWORD')
        self.RDS_USER = os.getenv('RDS_USER')
        self.RDS_DATABASE = os.getenv('RDS_DATABASE')
        self.RDS_PORT = os.getenv('RDS_PORT')

        self.DB_HOST = os.getenv('DB_HOST')
        self.DB_PASSWORD = os.getenv('DB_PASSWORD')
        self.DB_USER = os.getenv('DB_USER')
        self.DB_DATABASE = os.getenv('DB_DATABASE')
        self.DB_PORT = os.getenv('DB_PORT')

    # def read_db_creds(self):
    #     '''Reads the AWS Database Credentials'''
    #     with open("db_creds.yaml", 'r') as f:
    #         db_creds = yaml.safe_load(f)
    #         return (db_creds)
            
    # def local_creds(self):
    #     '''Reads the Local Database Credentials'''
    #     with open("local_db_creds.yaml", 'r') as f:
    #         local_db_creds = yaml.safe_load(f)
    #         return local_db_creds
             
    def init_db_engine(self):
        '''Creates an instance of the Engine Class'''
        #db_url = f'postgresql://{aws_creds["RDS_USER"]}:{aws_creds["RDS_PASSWORD"]}@{aws_creds["RDS_HOST"]}:{aws_creds["RDS_PORT"]}/{aws_creds["RDS_DATABASE"]}'
        db_url = f'postgresql://{self.RDS_USER}:{self.RDS_PASSWORD}@{self.RDS_HOST}:{self.RDS_PORT}/{self.RDS_DATABASE}'
        engine = create_engine(db_url) # Creates an instance of the sqlalchemy Engine class
        return engine
        
    def list_db_tables(self,engine):
        '''Connects to AWS RDS and lists table names'''
        connection = engine.connect() # Makes a connection to the database
        inspector = inspect(connection)
        print (inspector.get_table_names()) # Prints tables names

    def upload_to_db(self, df, table_name):
        '''Upload dataframe into local database'''
        local_db_url = f'postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_DATABASE}'
        local_engine = create_engine(local_db_url) # Create an engine
        local_connection = local_engine.connect() # Make a connection
        df.to_sql(table_name, local_connection, if_exists = 'replace') # Upload clean dataframe into local sales_data 
        print("upload done")
        