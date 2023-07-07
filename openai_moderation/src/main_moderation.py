import openai
import os
from dotenv import load_dotenv
import json
from db_connection import DbConnection

BATCH_SIZE = 1

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
    LIMIT 5
'''

# create cursor for getting data from the database
cur = Connection.conn.cursor()
cur.execute(query)

# load first batch of rows - if nothing, it just goes to the end of script
rows = cur.fetchmany(BATCH_SIZE)

i = 0
while rows:
    data = []
    for row in rows:
        text = row[1]
        
        response = openai.Moderation.create(
            input=text
        )

        output = response["results"][0]
        categories = json.loads(str(output['categories']))
        category_scores = json.loads(str(output['category_scores']))
        data.append({'id': int(row[0]),
                     'flagged': output['flagged'],
                     'categories': json.dumps(categories),
                     'category_scores': json.dumps(category_scores)})
    
    update_statement = '''
    UPDATE test_all_platforms.posts
    SET flagged = %(flagged)s,
        categories = %(categories)s
    WHERE _id = %(id)s'''

    Connection.updated_in_batches(update_statement, data)

    i += 1
    print(f'Done {i*BATCH_SIZE} posts')

    # load next batch of rows
    rows = cur.fetchmany(BATCH_SIZE)

Connection.commit()

# close connection after everything is done
cur.close()
Connection.close_connection()