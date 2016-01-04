
from slugify import slugify

def slug_provider(doc):
    return slugify(doc['name'], word_boundary=False, save_order=True)

def slug_dataset(doc):
    txt = "-".join([doc['provider'], doc['datasetCode']])
    return slugify(txt, word_boundary=False, save_order=True)

def slug_series(doc):
    txt = "-".join([doc['provider'], doc['datasetCode'], doc['key']])
    return slugify(txt, word_boundary=False, save_order=True)

