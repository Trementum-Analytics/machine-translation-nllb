from transformers.pipelines.pt_utils import KeyDataset
from datasets import Dataset
from text_preprocessing import clean_text
from translator import Translator
from db_connection import DbConnection
from dotenv import load_dotenv
import more_itertools
import os
import time
import re
from tqdm import tqdm

load_dotenv()
BATCH_SIZE = 1024
SCHEMA_NAME = os.getenv('SCHEMA_NAME')

# start database connection
try:
    # initialize connection to database
    Connection = DbConnection()
    Connection.connect()
    
    print('Connection successful')
except Exception as error:
    print('Connection error: ', error)

# initilalize the model
try:
    src_lang = 'spa_Latn'
    tgt_lang = 'eng_Latn'
    max_length = 400

    NllbTranslator = Translator(src_lang=src_lang, tgt_lang=tgt_lang)
except Exception as error:
    print('Model is not initialized: ', error)
    exit(2)

# load data for translation
query = f'''
    SELECT es._id, p.text_original
    FROM {SCHEMA_NAME}.openai_moderation_es_translated es
    JOIN {SCHEMA_NAME}.posts p
        ON es._id = p._id
    WHERE p.text_original IS NOT NULL
        AND es.text_eng IS NULL
    ORDER BY LENGTH(p.text_original)
'''

# create cursor for getting data from the database
cur = Connection.conn.cursor()
cur.execute(query)

# load first batch of rows - if nothing, it just goes to the end of script
rows = cur.fetchmany(BATCH_SIZE)

# Create a regular expression pattern with the values as separators
delimiters = ['? ', '! ', '. ']
pattern = '|'.join(map(re.escape, delimiters))

# try data stream
def data(dataset):
    for row in dataset:
        yield row["texts"]

while rows:
    
    merged_result = []
    ids = []
    output = []
    charachters_to_translate = 0
    start_time = time.time()
    
    # loop on this batch, create dataset from dict with text and ids, prepare for translation
    for row in rows:
        text = clean_text(row[1])
        charachters_to_translate += len(text)

        # Split the sentence using the pattern and keep the separators
        result = re.split(f'({pattern})', text)

        # Merge the separators with the subsequent elements
        result = [result[i] + result[i+1][0]
                        for i in range(0, len(result)-1, 2)] + [result[-1]]
                
        merged_result = merged_result + result
        ids = ids + [row[0] for i in range(len(result))]
    
    # dict to input for huggingface dataset
    dict_input = {'texts': merged_result,
                  'ids': ids}
    
    small_batch = 32

    dataset = Dataset.from_dict(dict_input)
    for out in tqdm(NllbTranslator.translation_pipeline(KeyDataset(dataset, "texts"), batch_size=small_batch, truncation="only_first"), total=len(merged_result)):
        output.append(out[0]['translation_text'])

    # # dataset = Dataset.from_dict(dict_input) no need for now
    # for chunk in more_itertools.chunked(dict_input['texts'], small_batch):
    #     translated_array = NllbTranslator.translate_batch(chunk, max_length=max_length)
    #     output = output + translated_array

    dict_output = {'texts': output,
                   'ids': ids}
    
    # merge all short sentences into full comments/posts
    short_dict = {}
    for id_val, text_val in zip(dict_output['ids'], dict_output['texts']):
        if id_val not in short_dict:
            short_dict[id_val] = text_val
        else:
            short_dict[id_val] += ' '
            short_dict[id_val] += text_val

    short_dict = {'id': list(short_dict.keys()), 'text': list(short_dict.values())}

    print('Text ready for loading')
    # prepare dict for database input
    dict_output = [dict(zip(short_dict, t)) for t in zip(*short_dict.values())]
    
    update_statement = f'UPDATE {SCHEMA_NAME}.openai_moderation_es_translated' + '''
        SET text_eng = %(text)s
        WHERE _id = %(id)s'''
    
    Connection.updated_in_batches(update_statement, dict_output)
    Connection.commit()
    
    print('Uploaded')
    
    # next batch
    rows = cur.fetchmany(BATCH_SIZE)

# close connection after everything is done
cur.close()
Connection.close_connection()