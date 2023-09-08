from dotenv import load_dotenv
import os
import psycopg2
from psycopg2 import extras


class DbConnection():
    """Class for all database functionality - creating connection and updating table in batches
    """
    def __init__(self) -> None:
        """Loads credentials
        """
        load_dotenv()

        self.HOST = os.getenv('POSTGRES_HOST')
        self.DB_NAME = os.getenv('POSTGRES_DATABASE')
        self.USERNAME = os.getenv('POSTGRES_USERNAME')
        self.PWD = os.getenv('POSTGRES_PASSWORD')
        self.PORT = os.getenv('POSTGRES_PORT')
    
    # create connection to the database
    def connect(self):
        """Creates a database connection using credentials
        """
        self.conn = psycopg2.connect(
            host=self.HOST,
            port=self.PORT,
            database=self.DB_NAME,
            user=self.USERNAME,
            password=self.PWD
        )
    
    # commit changes
    def commit(self):
        """Repeat of commit functionality for database connection
        """
        self.conn.commit()
        
    # close connection in the end
    def close_connection(self):
        """Close connection, should be performed after all the steps in the process done
        """
        self.conn.close()
        
    def updated_in_batches(self, update_statement: str, data: list):
        """Provides update statement in a batch - in this case, from the list

        Args:
            update_statement (str): pre-defined SQL update statement
            data (list): list for values to be updated in the table. One of the elements should be 'id' to have unique identifier
        """
        cur_update = self.conn.cursor()
        extras.execute_batch(cur_update, update_statement, data)
        cur_update.close()