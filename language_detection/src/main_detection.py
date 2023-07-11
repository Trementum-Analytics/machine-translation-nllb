from language_detection.src.langdetect import LanguageDetection
from db_connection import DbConnection
import os
from dotenv import load_dotenv

# load environmental schema name and table
load_dotenv()
DB_SCHEMA = os.getenv('DB_SCHEMA')
DB_TABLE = os.getenv('DB_TABLE')
BATCH_SIZE = 1024

# need to add model from fb - wget https://dl.fbaipublicfiles.com/nllb/lid/lid218e.bin
# also available in spaces
pretrained_lang_model = "content/lid218e.bin"

# initialize language detection with pretrained model
LangRecognize = LanguageDetection(pretrained_lang_model=pretrained_lang_model)

try:
    # initialize connection to database
    Connection = DbConnection()
    Connection.connect()
    
    print('Connection successful')
except Exception as error:
    print('Connection error: ', error)

# test row from db
query = f'''
    SELECT _id, text_original
    FROM {DB_SCHEMA}.{DB_TABLE}
    WHERE text_original IS NOT NULL
        AND lang_detect IS NOT NULL
'''
print(query)

# create cursor for getting data from the database
cur = Connection.conn.cursor()
cur.execute(query)

# load first batch of rows - if nothing, it just goes to the end of script
rows = cur.fetchmany(BATCH_SIZE)

i = 0 # just to check, to be deleted after all tests or transformed to another output
while rows:
    data = []
    
    for row in rows:
        data.append({'id': int(row[0]), 'text': row[1], 'lang': LangRecognize.detect(row[1])})
    
    update_statement = f'UPDATE {DB_SCHEMA}.{DB_TABLE}' + '''
        SET lang_detect = %(lang)s
        WHERE _id = %(id)s
    '''
    
    Connection.updated_in_batches(update_statement, data)

    i += 1
    print(f'Done {i*BATCH_SIZE} posts')
    
    # load next batch of rows
    rows = cur.fetchmany(BATCH_SIZE)
    
Connection.commit()
    
# close connection after everything is done
cur.close()
Connection.close_connection()