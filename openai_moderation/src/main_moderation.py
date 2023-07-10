import openai
import os
from dotenv import load_dotenv
import json
from db_connection import DbConnection

BATCH_SIZE = 1000

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
'''

# create cursor for getting data from the database
cur = Connection.conn.cursor()
cur.execute(query)

# load first batch of rows - if nothing, it just goes to the end of script
rows = cur.fetchmany(BATCH_SIZE)

i = 0
while rows:
    data = []
    input_texts = []
    ids = []
    
    # prepare list of texts for openai moderation
    for row in rows:
        ids.append(row[0])
        input_texts.append(row[1])
    
    #make a query    
    response = openai.Moderation.create(
        input=input_texts
    )

    for i, output in enumerate(response['results']):
        categories = json.loads(str(output['categories']))
        category_scores = json.loads(str(output['category_scores']))
    
        # created list of dictionaries - for each row in database with columns for each score parameter
        data.append({'id': int(row[ids[i]]),
                        'flagged': output['flagged'],
                        'categories': json.dumps(categories),
                        'sexual': category_scores['sexual'],
                        'hate': category_scores['hate'],
                        'harassment': category_scores['harassment'],
                        'selfharm': category_scores['self-harm'],
                        'sexual_minors': category_scores['sexual/minors'],
                        'hate_threatening': category_scores['hate/threatening'],
                        'violence_graphic': category_scores['violence/graphic'],
                        'selfharm_intent': category_scores['self-harm/intent'],
                        'selfharm_instructions': category_scores['self-harm/instructions'],
                        'harassment_threatening': category_scores['harassment/threatening'],
                        'violence': category_scores['violence']})
    
    update_statement = '''
    UPDATE test_all_platforms.posts
    SET flagged = %(flagged)s,
        categories = %(categories)s,
        sexual = %(sexual)s,
        hate = %(hate)s,
        harassment = %(harassment)s,
        selfharm = %(selfharm)s,
        sexual_minors = %(sexual_minors)s,
        hate_threatening = %(hate_threatening)s,
        violence_graphic = %(violence_graphic)s,
        selfharm_intent = %(selfharm_intent)s,
        selfharm_instructions = %(selfharm_instructions)s,
        harassment_threatening = %(harassment_threatening)s,
        violence = %(violence)s
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