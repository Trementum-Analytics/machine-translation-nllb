from transformers.pipelines.pt_utils import KeyDataset
from datasets import Dataset
from text_preprocessing import clean_text
from translator import Translator
from db_connection import DbConnection
import re
from tqdm import tqdm

BATCH_SIZE = 1024
STRATEGY = 'comment'

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

    MTranslator = Translator(src_lang=src_lang, tgt_lang=tgt_lang)
except Exception as error:
    print('Model is not initialized: ', error)
    exit(2)

# perform select to table
Connection.execute_select(strategy=STRATEGY)
# get rows from select statement
rows = Connection.fetch_rows(BATCH_SIZE=BATCH_SIZE)

# Create a regular expression pattern with the values as separators
delimiters = ['? ', '! ', '. ']
pattern = '|'.join(map(re.escape, delimiters))

# data stream from huggingface dataset
def data(dataset):
    for row in dataset:
        yield row["texts"]

while rows:
    
    merged_result = []
    ids = []
    output = []
    charachters_to_translate = 0
    
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
    for out in tqdm(MTranslator.opus_pipe(KeyDataset(dataset, "texts"), batch_size=small_batch, truncation="only_first"), total=len(merged_result)):
        output.append(out[0]['translation_text'])

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
    
    Connection.updated_in_batches(data=dict_output,
                                  model_name=MTranslator.model_name,
                                  strategy=STRATEGY)
    Connection.commit()
        
    # next batch
    rows = Connection.fetch_rows(BATCH_SIZE=BATCH_SIZE)

# close connection after everything is done
Connection.close_connection()