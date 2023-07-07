import openai
import os
from dotenv import load_dotenv
from db_connection import DbConnection


# init openai api
load_dotenv()

openai.api_key = os.getenv('OPENAI_MODERATION')

# start database connection
try:
    # initialize connection to database
    Connection = DbConnection()
    Connection.connect()
    
    print('Connection successful')
except Exception as error:
    print('Connection error: ', error)
    
query = '''
    SELECT _id, text_original
    FROM test_all_platforms.posts
    WHERE text_original IS NOT NULL
        AND lang_detect IS NOT NULL
'''    

response = openai.Moderation.create(
    input="Sample text goes here"
)

output = response["results"][0]

print(output)