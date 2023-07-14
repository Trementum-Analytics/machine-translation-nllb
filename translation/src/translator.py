from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, pipeline
from torch import cuda

TASK = "translation"
# choices from small to large: nllb-200-distilled-600M, nllb-200-distilled-1.3B, nllb-200-1.3B, nllb-200-3.3B (usual 1.3B is optimal)

class Translator():
    def __init__(self, src_lang, tgt_lang , model_name="facebook/nllb-200-distilled-600M") -> None:
        self.src_lang = src_lang
        self.tgt_lang = tgt_lang
        self.model_name = model_name
        self.device = 0 if cuda.is_available() else -1

        # initialize the model
        self.model = AutoModelForSeq2SeqLM.from_pretrained(self.model_name).to(self.device)
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name, src_lang=src_lang)
        print('Model is ready')

    def translate_batch(self, array, max_length):
        inputs = self.tokenizer(array, return_tensors="pt", padding = True).to(self.device)

        translated_tokens = self.model.generate(
        **inputs, forced_bos_token_id=self.tokenizer.lang_code_to_id[self.tgt_lang], max_length=max_length
        )

        return self.tokenizer.batch_decode(translated_tokens, skip_special_tokens=True)