from langdetect import LanguageDetection
from db_connection import DbConnection

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

# create cursor for getting data from the database
cur = Connection.conn.cursor()
cur.execute(query)
print('Connection done')

# load first batch of rows - if nothing, it just goes to the end of script
rows = cur.fetchmany(1000)

i = 0 # just to check, to be deleted after
while rows:
    data = []
    
    for row in rows:
        data.append({'id': int(row[0]), 'text': row[1], 'lang': LangRecognize.detect(row[1])})
    
    update_statement = '''
        UPDATE test_all_platforms.posts
        SET lang_detect = %(lang)s
        WHERE _id = %(id)s'''
    
    Connection.updated_in_batches(update_statement, data)

    i += 1
    print(f'Done {i*1000} posts')
    
    # load next batch of rows
    rows = cur.fetchmany(1000)
    
Connection.commit()
    
# close connection after everything is done
cur.close()
Connection.close_connection()