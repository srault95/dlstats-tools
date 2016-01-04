# -*- coding: utf-8 -*-

"""
169.504 seconds pour 155 704 series (918,60 / seconde)
Pour 18 000 000 : / 918,60 = 19 595 secondes (326mn)

155 seconds avec patch gevent !

> Compter les doc à traiter:
    > db.series.count({"slug": {"$exists": false}})

> Annuler les modifications:
    > db.series.updateMany({"slug": {"$exists": true}}, {$unset: {"slug": ""}})
    { "acknowledged" : true, "matchedCount" : 5, "modifiedCount" : 5 }
"""

from gevent import monkey
monkey.patch_all()

import time
import gevent
from gevent import pool
from gevent.queue import Queue, Empty

from dlstats import utils
from dlstats import constants

import update_slug_common as common

def producer(queue, db, col, slug_func):
    for doc in db[col].find({"slug": {"$exists": False}}):
        query = {
            "col": col,
            "id": doc['_id'],
            "slug": slug_func(doc)
        }
        queue.put(query)
    
def update_doc(db, col, _id, query):
    return db[col].update_one({"_id": _id}, query)

def consumer(queue, db):
    p = pool.Pool(20)
    i = 0 
    while True:
        try:
            query = queue.get(timeout=2.0)
            i += 1
            p.spawn(update_doc, db, query['col'], query['id'], {"$set": {"slug": query["slug"]} })
        except Empty:
            print("Queue empty")
            break
    p.join()
    print("consumer count : ", i)

def main():
    import logging    
    logging.basicConfig(level=logging.DEBUG)
    db = utils.get_mongo_db()
    greenlets = []
    
    q = Queue()
    
    greenlets.append(gevent.spawn(producer, q, db, constants.COL_PROVIDERS, common.slug_provider))
    greenlets.append(gevent.spawn(producer, q, db, constants.COL_DATASETS, common.slug_dataset))
    greenlets.append(gevent.spawn(producer, q, db, constants.COL_SERIES, common.slug_series))
    greenlets.append(gevent.spawn(consumer, q, db))
    start = time.time()
    try:
        gevent.joinall(greenlets)
    except KeyboardInterrupt:
        pass
    end = time.time()
    print("%.3f seconds" % (end - start))
    
if __name__ == "__main__":
    main()    

