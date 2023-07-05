from langdetect import LanguageDetection
from db_connection import DbConnection

# need to add model from fb - wget https://dl.fbaipublicfiles.com/nllb/lid/lid218e.bin
# also available in spaces
pretrained_lang_model = "../content/lid218e.bin"

LangRecognize = LanguageDetection(pretrained_lang_model=pretrained_lang_model)

Connection = DbConnection()
Connection.connect()

# test row from db
query = '''
    SELECT _id, text_original
    FROM test_all_platforms.posts
    WHERE text_original IS NOT NULL
'''
cur = Connection.conn.cursor()
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