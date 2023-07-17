import openai
import os
import more_itertools
from dotenv import load_dotenv
import json
from db_connection import DbConnection

# load environment variables
load_dotenv()
DB_SCHEMA = os.getenv('DB_SCHEMA')
DB_TABLE = os.getenv('DB_TABLE')
BATCH_SIZE = 1024

openai.api_key = os.getenv('OPENAI_MODERATION')

# start database connection
try:
    # initialize connection to database
    Connection = DbConnection()
    Connection.connect()
    
    print('Connection successful')
except Exception as error:
    print('Connection error: ', error)
    exit(1)
    
# query is used on special table for openai moderation, where translation to english is already prepared    
query = f'''
    SELECT _id, text_eng
    FROM {DB_SCHEMA}.{DB_TABLE}
    WHERE text_eng IS NOT NULL
        AND flagged IS NULL
'''

# create cursor for getting data from the database
cur = Connection.conn.cursor()
cur.execute(query)

# load first batch of rows - if nothing, it just goes to the end of script
rows = cur.fetchmany(BATCH_SIZE)

# count interations to print progress
iterations = 0

# main loop - takes rows from query by batch size and performs operations until all rows are proceeded
while rows:
    # some data structures (data - prepared for upload, input texts - for model input, ids - posts ids for identification)
    data = []
    input_texts = []
    ids = []
    
    # prepare list of texts for openai moderation
    for row in rows:
        ids.append(row[0])
        input_texts.append(row[1])
    
    # create structure for results from moderation
    moderation_outputs = []
    
    # maximum input of array for model is 32, so split list with texts into equal chunks
    for chunk in more_itertools.chunked(input_texts, 32):
        response = openai.Moderation.create(
            input=chunk
        )
        
        # from results add to list with outputs
        for result in response['results']:
            moderation_outputs.append(result)

    # prepare data for uploading
    for i, output in enumerate(moderation_outputs):
        categories = json.loads(str(output['categories']))
        category_scores = json.loads(str(output['category_scores']))
    
        # created list of dictionaries - for each row in database with columns for each score parameter
        data.append({'id': int(ids[i]),
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
    
    # upload to database
    update_statement = f'UPDATE {DB_SCHEMA}.{DB_TABLE}' + '''
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

    # fast upload using batches
    Connection.updated_in_batches(update_statement, data)

    # print progress
    print(f'Done {(iterations * BATCH_SIZE) + len(data)} posts')
    iterations += 1    

    # load next batch of rows
    rows = cur.fetchmany(BATCH_SIZE)

    # commit after each iteration
    Connection.commit()

# close connection after everything is done
cur.close()
Connection.close_connection()