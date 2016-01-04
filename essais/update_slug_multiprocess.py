# -*- coding: utf-8 -*-

import time
import multiprocessing as mp

from dlstats import utils
from dlstats import constants

import update_slug_common as common

def producer(queue):
    db = utils.get_mongo_db()
    for doc in db[constants.COL_PROVIDERS].find({"slug": {"$exists": False}}):
        query = {
            "col": constants.COL_PROVIDERS,
            "id": doc['_id'],
            "slug": common.slug_provider(doc)
        }
        print("PRO : ", query)
        queue.put(query)
    
    return "TEST"

def consumer(queue):
    db = utils.get_mongo_db()
    while True:
        try:
            query = queue.get()
            if query is None:
                break
            print("CONS : ", query)
            queue.task_done()
        except TimeoutError:
            print("TimeoutError")
    queue.task_done()        

def main():
    import logging
    
    logging.basicConfig(level=logging.DEBUG)
    db = utils.get_mongo_db()
    
    #mp.set_start_method('spawn')
    m = mp.Manager()
    q = m.JoinableQueue()
        
    pool = mp.Pool(processes=4)
    results = []    
    try:
        results.append(pool.apply_async(producer, [q]))
        results.append(pool.apply_async(consumer, [q]))
        #print(dir(pool))
        #print(dir(result))
        #print("result : ", result.get())
        for r in results:
            print(r.get())
        
        while not q.empty(): # wait for processing to finish
            time.sleep(0.1)         
        
    finally:
        pool.close()
        pool.join()

    """

    q = mp.Queue()
    p_prod = mp.Process(target=producer, args=[q])
    p_cons = mp.Process(target=consumer, args=[q])
    
    try:    
        p_prod.start()
        p_cons.start()
        
        p_prod.join()
        p_cons.join()
        
    except KeyboardInterrupt:
        pass
    """

if __name__ == "__main__":
    main()