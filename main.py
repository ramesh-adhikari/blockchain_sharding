
import os
import time
from generator_scripts.bootstrap import Bootstrap
from logger import generate_log
from models.transaction import Transaction
import sys
import multiprocessing
from client import init_client
from models.shard import Shard
from server import init_server
from shard import *
from config import *

processes = []

def parallel_transactions_processing():
    shards = generate_shards()
    port = INITAIL_PORT
    for shard in shards:
        if(shard.is_leader):
            init_process(True,shard.id,port) #server
            init_clients(shards,port)
            port += 1

    for process in processes:
        process.join()

def init_process(server,shard_id, port):
    p = multiprocessing.Process(target=init_server if server else init_client, args=(shard_id,port,))
    processes.append(p)
    p.start()

def init_clients(shards,port):
    for shard in shards:
        init_process(False,shard.id,port) #client

if __name__ == '__main__':

    start_time = time.time()

    if (len( sys.argv ) > 1):
        print("Erase old transactions and generate new transactions and process these transactions")
        Bootstrap.run()
        parallel_transactions_processing()
    else:
        if(os.path.exists(os.path.abspath(os.curdir)+'/storages')):
            print('Start processing existing transactions!')
            parallel_transactions_processing()
        else:
            print('Transactions and storages not found please run the command with extra parameters like: "python3 main.py storage"')
            exit()

    generate_log(start_time)


