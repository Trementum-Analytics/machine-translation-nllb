import fasttext
from dotenv import load_dotenv
import os
import psycopg2

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

# need to add model from fb - wget https://dl.fbaipublicfiles.com/nllb/lid/lid218e.bin
# also available in spaces
pretrained_lang_model = "../content/lid218e.bin"
model = fasttext.load_model(pretrained_lang_model)


# test row from db
cur = conn.cursor()

cur.execute('SELECT text_original FROM test_all_platforms.posts WHERE text_original IS NOT NULL LIMIT 1 OFFSET 6')
res = cur.fetchall()

for row in res:
    text = row[0].replace('\n', ' ')

# find source language for this text
predictions = model.predict(text, k=1)
src_lang = predictions[0][0].replace('__label__', '')
print(text)
print(src_lang)