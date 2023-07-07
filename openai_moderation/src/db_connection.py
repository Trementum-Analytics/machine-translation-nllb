from dotenv import load_dotenv
import os
import psycopg2
from psycopg2 import extras


class DbConnection():
    def __init__(self) -> None:
        # get credentials for database connection
        load_dotenv()

        self.HOST = os.getenv('POSTGRES_HOST')
        self.DB_NAME = os.getenv('POSTGRES_DATABASE')
        self.USERNAME = os.getenv('POSTGRES_USERNAME')
        self.PWD = os.getenv('POSTGRES_PASSWORD')
        self.PORT = os.getenv('POSTGRES_PORT')
    
    # create connection to the database
    def connect(self):
        # connect to database
        self.conn = psycopg2.connect(
            host=self.HOST,
            port=self.PORT,
            database=self.DB_NAME,
            user=self.USERNAME,
            password=self.PWD
        )
    
    # commit changes
    def commit(self):
        self.conn.commit()
        
    # close connection in the end
    def close_connection(self):
        self.conn.close()
        
    def updated_in_batches(self, update_statement, data):
        cur_update = self.conn.cursor()
        extras.execute_batch(cur_update, update_statement, data)