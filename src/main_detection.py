from langdetect import LanguageDetection
from db_connection import DbConnection
from psycopg2 import extras

# need to add model from fb - wget https://dl.fbaipublicfiles.com/nllb/lid/lid218e.bin
# also available in spaces
pretrained_lang_model = "../content/lid218e.bin"

# initialize language detection with pretrained model
LangRecognize = LanguageDetection(pretrained_lang_model=pretrained_lang_model)

# initialize connection to database
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

i = 0
while rows:
    cur_update = Connection.conn.cursor()
    data = []
    
    for row in rows:
        data.append({'id': int(row[0]), 'text': row[1], 'lang': LangRecognize.detect(row[1])})
    
    update_statement = '''
        UPDATE test_all_platforms.posts
        SET lang_detect = %(lang)s
        WHERE _id = %(id)s'''
    
    extras.execute_batch(cur_update, update_statement, data)

    
    i += 1
    print(f'Done {i*1000} posts')
    
    rows = cur.fetchmany(1000)
    
Connection.conn.commit()
    
# close connection after everything is done
cur.close()
Connection.conn.close()