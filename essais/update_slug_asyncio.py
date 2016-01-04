# -*- coding: utf-8 -*-

import asyncio

from dlstats import utils
from dlstats import constants

import update_slug_common as common

queue = None

@asyncio.coroutine
def producer(db):
    for doc in db[constants.COL_PROVIDERS].find({"slug": {"$exists": False}}):
        query = {
            "col": constants.COL_PROVIDERS,
            "id": doc['_id'],
            "slug": common.slug_provider(doc)
        }
        print("PRO : ", query)
        yield from queue.put(query)

@asyncio.coroutine
def consumer():
    while True:
        query = yield from queue.get()
        print("CONS : ", query)

def main():
    import os
    import logging

    global queue
    
    #set PYTHONASYNCIODEBUG=1
    
    logging.basicConfig(level=logging.DEBUG)
    db = utils.get_mongo_db()
    
    if os.name == 'nt':        
        io_loop = asyncio.ProactorEventLoop()
        asyncio.set_event_loop(io_loop)
    else:
        io_loop = asyncio.get_event_loop()
    
    # Avec size 2: traite 3 par 3
    queue = asyncio.Queue(maxsize=2, loop=io_loop)
    
    ensure_future = asyncio.async        
    tasks = []
    try:
        tasks.append(ensure_future(producer(db)))
        tasks.append(ensure_future(consumer()))
        io_loop.run_until_complete(asyncio.wait(tasks))
        print("APRES until...")
        #io_loop.run_until_complete(producer(db))
        #io_loop.run_until_complete(consumer())
        io_loop.close()
    except KeyboardInterrupt:
        pass
    
    print(queue.qsize())  

if __name__ == "__main__":
    main()