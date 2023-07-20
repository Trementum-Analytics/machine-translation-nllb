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
        self.cur.close()
        self.conn.close()
    
    # takes update statement and updates data in the database, without commit
    def updated_in_batches(self, data):
        # define statement
        update_statement = f'UPDATE {self.SCHEMA_NAME}.openai_moderation_es_translated' + '''
        SET text_eng = %(text)s
        WHERE _id = %(id)s
            AND type = 'post'
        '''
        
        # cursor for update
        cur_update = self.conn.cursor()
        extras.execute_batch(cur_update, update_statement, data)

    # just change select statement here if needed
    def execute_select (self):
        # define schema name
        self.SCHEMA_NAME = os.getenv('SCHEMA_NAME')
        
        # load data for translation
        query = f'''
            SELECT es._id, p.text_original
            FROM {self.SCHEMA_NAME}.openai_moderation_es_translated es
            JOIN {self.SCHEMA_NAME}.posts p
                ON es._id = p._id
            WHERE p.text_original IS NOT NULL
                AND es.text_eng IS NULL
                AND es.type = 'post'
        '''
        
        # create cursor for getting data from the database
        self.cur = self.conn.cursor()
        self.cur.execute(query)
    
    # returns the number of rows, defined as BATCH_SIZE
    def fetch_rows(self, BATCH_SIZE=1024):
        # load first batch of rows - if nothing, it just goes to the end of script
        rows = self.cur.fetchmany(BATCH_SIZE)
        
        return rows