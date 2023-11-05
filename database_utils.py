import yaml
from sqlalchemy import create_engine, text, inspect


class DatabaseConnector():
    '''This function reads databases credentials, initialise engines, list table names, and upload clean data into local database'''

    def read_db_creds(self):
        '''Reads the AWS Database Credentials'''
        with open("db_creds.yaml", 'r') as f:
            db_creds = yaml.safe_load(f)
            return db_creds
    
    def local_creds(self):
        '''Reads the Local Database Credentials'''
        with open("local_db_creds.yaml", 'r') as f:
            local_db_creds = yaml.safe_load(f)
            return local_db_creds
             
    def init_db_engine(self, aws_creds):
        '''Creates an instance of the Engine Class'''
        db_url = f'postgresql://{aws_creds["RDS_USER"]}:{aws_creds["RDS_PASSWORD"]}@{aws_creds["RDS_HOST"]}:{aws_creds["RDS_PORT"]}/{aws_creds["RDS_DATABASE"]}'
        engine = create_engine(db_url) # Creates an instance of the sqlalchemy Engine class
        return engine
        
    def list_db_tables(self,engine):
        '''Connects to AWS RDS and lists table names'''
        connection = engine.connect() # Makes a connection to the database
        inspector = inspect(connection)
        print (inspector.get_table_names()) # Prints tables names

    def upload_to_db(self, df, table_name, local_creds):
        '''Upload dataframe into local database'''
        local_db_url = f'postgresql://{local_creds["DB_USER"]}:{local_creds["DB_PASSWORD"]}@{local_creds["DB_HOST"]}:{local_creds["DB_PORT"]}/{local_creds["DB_DATABASE"]}'
        local_engine = create_engine(local_db_url) # Create an engine
        local_connection = local_engine.connect() # Make a connection
        df.to_sql(table_name, local_connection, if_exists = 'replace') # Upload clean dataframe into local sales_data 
        print("upload done")
        