from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, pipeline
from transformers.pipelines.pt_utils import KeyDataset
from datasets import Dataset
from text_preprocessing import clean_text
from torch import cuda
from db_connection import DbConnection
import re

BATCH_SIZE = 1000

# start database connection
try:
    # initialize connection to database
    Connection = DbConnection()
    Connection.connect()
    
    print('Connection successful')
except Exception as error:
    print('Connection error: ', error)

# load data for translation
query = '''
    SELECT _id, text_original
    FROM test_all_platforms.posts
    WHERE text_original IS NOT NULL
    AND lang = 'es'
    LIMIT 1
'''

# create cursor for getting data from the database
cur = Connection.conn.cursor()
cur.execute(query)

# load first batch of rows - if nothing, it just goes to the end of script
rows = cur.fetchmany(BATCH_SIZE)

device = 0 if cuda.is_available() else -1

TASK = "translation"
CKPT = "facebook/nllb-200-distilled-600M"

model = AutoModelForSeq2SeqLM.from_pretrained(CKPT)
tokenizer = AutoTokenizer.from_pretrained(CKPT)

src_lang = 'spa_Latn'
tgt_lang = 'eng_Latn'
max_length = 400

translation_pipeline = pipeline(TASK,
                                model=model,
                                tokenizer=tokenizer,
                                src_lang=src_lang,
                                tgt_lang=tgt_lang,
                                max_length=max_length,
                                device=device)

print('Pipeline initiated')

# Create a regular expression pattern with the values as separators
delimiters = ['? ', '! ', '. ']
pattern = '|'.join(map(re.escape, delimiters))

while rows:
    
    merged_result = []
    ids = []
    output = []
    
    # loop on this batch, create dataset from dict with text and ids, prepare for translation
    for row in rows:
        text = clean_text(row[1])

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
    
    dataset = Dataset.from_dict(dict_input)
    for out in translation_pipeline(KeyDataset(dataset, "texts"), batch_size=1, truncation="only_first"):
        output.append(out[0]['translation_text'])
    print('Text translated')
        
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
    
    update_statement = '''
        UPDATE test_all_platforms.posts
        SET eng_translation = %(text)s
        WHERE _id = %(id)s'''
    
    Connection.updated_in_batches(update_statement, dict_output)
    Connection.commit()
    
    print('Uploaded')
    
    # next batch
    rows = cur.fetchmany(BATCH_SIZE)