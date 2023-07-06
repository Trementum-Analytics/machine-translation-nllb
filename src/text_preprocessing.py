import re
from cleantext import clean

# regex for detection of urls in text
full_regex = r'((?<=[^a-zA-Z0-9])(?:https?\:\/\/|[a-zA-Z0-9]{1,}\.{1}|\b)(?:\w{1,}\.{1}){1,5}(?:com|org|edu|gov|uk|net|ca|de|jp|fr|au|us|ru|ch|it|nl|se|no|es|mil|iq|io|ac|ly|sm){1}(?:\/[a-zA-Z0-9]{1,})*)'

# check for letters in string
def has_alphabetical_letters(text):
    pattern = r'[^\W\d_]'  # Matches any letter in any language
    return bool(re.search(pattern, text))

# full clean text function
def clean_text(document):

    final_document = ""

    # replace links - miltiple types of links and @address for tweets
    document = re.sub(r'http\S+', '', document)
    document = re.sub(r'https\S+', '', document)
    document = re.sub(full_regex, '', document)
    document = re.sub(r'www\S+', '', document)
    document = re.sub(r'(@[A-Za-z0–9_]+)', '', document)

    # remove emojis
    document = clean(document, fix_unicode=False, to_ascii=False,
                     lower=False, no_emoji=True, no_punct=False)

    # remove new lines and replace with space
    document = re.sub(r'\n', ' ', document)

    # split and clear from unnecessary items
    document = document.split()
    useless_words = [':=:', ' More:']
    text_filtered = [word for word in document if not word in useless_words]

    final_document = ' '.join(text_filtered)

    # remove extra spaces
    final_document = final_document.strip()
    
    if has_alphabetical_letters(final_document):
        return final_document
    else:
        return None

