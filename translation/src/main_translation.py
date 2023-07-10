from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, pipeline
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

# Create a regular expression pattern with the values as separators
delimiters = ['? ', '! ', '. ']
pattern = '|'.join(map(re.escape, delimiters))

for row in rows:
    text = row[1]

# Split the sentence using the pattern and keep the separators
result = re.split(f'({pattern})', text)

# Merge the separators with the subsequent elements
merged_result = [result[i] + result[i+1][0]
                 for i in range(0, len(result)-1, 2)] + [result[-1]]

res = ' '.join([translation_pipeline(res)[0]['translation_text']
               for res in merged_result])

print(text)
print(res)