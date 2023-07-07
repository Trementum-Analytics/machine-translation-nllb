import fasttext
from language_detection.src.text_preprocessing import clean_text

class LanguageDetection():
    def __init__(self, pretrained_lang_model) -> None:
        self.detection_model = fasttext.load_model(pretrained_lang_model)
    
    def detect(self, text):
        
        # replace new lines - also need to be done some more preprocessing
        text = text.replace('\n', ' ')
        text = clean_text(text)
        
        if text == '' or text is None:
            return 'und'
        else:
            # find source language for this text
            predictions = self.detection_model.predict(text, k=1)
            lang = predictions[0][0].replace('__label__', '')
            
            return lang