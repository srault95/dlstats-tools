# -*- coding: utf-8 -*-

"""
- pymongo 3.1.1 - mongodb 3.2 wireTiger
    39 seconds avec gevent
    42 seconds pour 155 704 series : bulk 20                            3707/s
    Pour 18 000 000 : / 3707 = 4 855 secondes (80mn)

    36 seconds: bulk 200
    > Avec un print(doc) et bulk 1000: 136 secondes !!!
    
    26 seconds: bulk 1000:                                              5988/s
    pour 18 millions: 50 mn

- pymongo 3.2 - mongodb 3.2 wireTiger
    indexes foreground:

    bulk_write(bypass_document_validation=False) - pas d'index
    33 second: bulk 1000 : 162 347 docs                                 4919/s
    
    bulk_write(bypass_document_validation=True)
    38 second: bulk 1000 : 162 347 docs                                 4272/s
    
    sparse indexe sur slug: 41 second                                   3959/s

    indexes background + sparse : new index : 61 seconds                2261/s
    
    indexes background + sparse : exist index : 61 seconds              2261/s

    indexes foreground + sparse : new index : 52 seconds                ?/s

    indexes foreground + sparse : exist index : 51 seconds               ?/s
        
    Retour foreground + no sparse: new index:

    Retour foreground + no sparse - Not unique : 56 seconds  

- pymongo 3.1.1 - mongodb 3.2 wireTiger
    
    Retour foreground + no sparse - Not unique : ?  
 

> Compter les doc à traiter:
    > db.series.count({"slug": {"$exists": false}})

> Annuler les modifications:
    > db.series.dropIndex("slug_idx")
    { "nIndexesWas" : 11, "ok" : 1 }

    > db.series.updateMany({"slug": {"$exists": true}}, {$unset: {"slug": ""}})
    { "acknowledged" : true, "matchedCount" : 5, "modifiedCount" : 5 }
    
    > db.series.reIndex()
    
    > Relancer process
    
    #> db.series.createIndex({"slug": 1}, {"name": "slug_idx", "unique": true, "background": true})

"""

import time

from pprint import pprint
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne

from slugify import slugify

from dlstats import utils
from dlstats import constants

def slug_provider(doc):
    return slugify(doc['name'], word_boundary=False, save_order=True)

def slug_dataset(doc):
    txt = "-".join([doc['provider'], doc['datasetCode']])
    return slugify(txt, word_boundary=False, save_order=True)

def slug_series(doc):
    txt = "-".join([doc['provider'], doc['datasetCode'], doc['key']])
    return slugify(txt, word_boundary=False, save_order=True)


def consumer(db, col, slug_func, projection, limit):
    try:
        requests = []
        #, cursor_type=CursorType.EXHAUST : pas compatible mongos et sharding
        for doc in db[col].find({"slug": {"$exists": False}}, projection=projection):
            slug = slug_func(doc)
            #print(doc)
            if len(requests) == limit:
                db[col].bulk_write(requests)
                requests = []
            requests.append(UpdateOne({"_id": doc["_id"]}, {"$set": {"slug": slug}}))
        
        if len(requests) > 0:
            db[col].bulk_write(requests)
        
    except BulkWriteError as bwe:
        pprint(bwe.details)

def main():
    import logging    
    logging.basicConfig(level=logging.DEBUG)
    db = utils.get_mongo_db()
    
    utils.create_or_update_indexes(db)
    
    start = time.time()
    try:        
        
        _start = time.time()
        consumer(db, constants.COL_PROVIDERS, slug_provider, {"name": True}, 1000)
        print("%s - %.3f seconds" % (constants.COL_PROVIDERS, (time.time() - _start)))
        
        _start = time.time()
        consumer(db, constants.COL_DATASETS, slug_dataset, {"provider": True, "datasetCode": True}, 1000)
        print("%s - %.3f seconds" % (constants.COL_DATASETS, (time.time() - _start)))
        
        _start = time.time()
        consumer(db, constants.COL_SERIES, slug_series, {"provider": True, "datasetCode": True, "key": True}, 1000)
        print("%s - %.3f seconds" % (constants.COL_SERIES, (time.time() - _start)))
        
        #TODO: il faut un reindex à la fin sur chaque series modifié ???
        """
        Migration:
        - 1. Lancer mise à jour slug
        - 2. utils.create_or_update_indexes(db)
        """
        
    except KeyboardInterrupt:
        pass
    end = time.time()
    print("%.3f seconds" % (end - start))
    
if __name__ == "__main__":
    main()    


