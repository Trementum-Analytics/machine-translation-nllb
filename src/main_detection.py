from dotenv import load_dotenv
import os
import psycopg2
from langdetect import LanguageDetection

# need to add model from fb - wget https://dl.fbaipublicfiles.com/nllb/lid/lid218e.bin
# also available in spaces
pretrained_lang_model = "../content/lid218e.bin"

LangRecognize = LanguageDetection(pretrained_lang_model=pretrained_lang_model)

# get credentials for database connection
load_dotenv()

HOST = os.getenv('POSTGRES_HOST')
DB_NAME = os.getenv('POSTGRES_DATABASE')
USERNAME = os.getenv('POSTGRES_USERNAME')
PWD = os.getenv('POSTGRES_PASSWORD')
PORT = os.getenv('POSTGRES_PORT')

# connect to database
conn = psycopg2.connect(
    host=HOST,
    port=PORT,
    database=DB_NAME,
    user=USERNAME,
    password=PWD
)

# test row from db
query = '''
    SELECT _id, text_original
    FROM test_all_platforms.posts
    WHERE text_original IS NOT NULL
'''
cur = conn.cursor()
cur.execute(query)

rows = cur.fetchmany(1000)

# while rows:
ids = []
texts = []

for row in rows:
    ids.append(row[0])
    texts.append(row[1])
        
src_lang = LangRecognize.detect(texts[0])

print(texts[0])
print(src_lang)