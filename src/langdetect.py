import fasttext

class LanguageDetection():
    def __init__(self, pretrained_lang_model) -> None:
        self.detection_model = fasttext.load_model(pretrained_lang_model)
    
    def detect(self, text):
        
        # replace new lines
        text = text.replace('\n', ' ')
        
        # find source language for this text
        predictions = self.detection_model.predict(text, k=1)
        lang = predictions[0][0].replace('__label__', '')
        
        return lang