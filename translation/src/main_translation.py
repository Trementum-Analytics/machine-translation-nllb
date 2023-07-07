from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, pipeline
from torch import cuda
import re

device = 0 if cuda.is_available() else -1

TASK = "translation"
CKPT = "facebook/nllb-200-distilled-600M"

model = AutoModelForSeq2SeqLM.from_pretrained(CKPT)
tokenizer = AutoTokenizer.from_pretrained(CKPT)

src_lang = 'ita_Latn'
tgt_lang = 'eng_Latn'
max_length = 400

translation_pipeline = pipeline(TASK,
                                model=model,
                                tokenizer=tokenizer,
                                src_lang=src_lang,
                                tgt_lang=tgt_lang,
                                max_length=max_length,
                                device=device)

text = '@AgainCarlakak Ma che cazz sta dicendo? Perché hanno la  demenza è devono morire!! ma sto faccia di merda lo dice strac…'
delimiters = ['? ', '! ', '. ']

# Create a regular expression pattern with the values as separators
pattern = '|'.join(map(re.escape, delimiters))

# Split the sentence using the pattern and keep the separators
result = re.split(f'({pattern})', text)

# Merge the separators with the subsequent elements
merged_result = [result[i] + result[i+1][0]
                 for i in range(0, len(result)-1, 2)] + [result[-1]]

res = ' '.join([translation_pipeline(res)[0]['translation_text']
               for res in merged_result])
