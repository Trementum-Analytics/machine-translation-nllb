from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, pipeline
from torch import cuda

TASK = "translation"
CKPT = "facebook/nllb-200-distilled-600M"