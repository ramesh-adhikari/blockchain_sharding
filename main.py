import os
import time
from generator_scripts.bootstrap import Bootstrap
from report import clear_reports, generate_report
import sys
import multiprocessing
from client import init_client
from server import init_server
from config import *

processes = []
start_time = 0

def init_process(server,shard_id, port,leader_shard_id):
    if(server):
        p = multiprocessing.Process(target=init_server, args=(shard_id,port))
    else:
        p = multiprocessing.Process(target=init_client, args=(shard_id,port,leader_shard_id))
    processes.append(p)
    p.start()

def init_clients(port,leader_shard_id):
    for shard in SHARDS:
        init_process(False,shard[0],port,leader_shard_id) #client

def parallel_transactions_processing():
    global start_time
    port = INITAIL_PORT
    for shard in SHARDS:
        if(shard[1]): # is leader
            init_process(True,shard[0],port,shard[0]) #server
            init_clients(port,shard[0])
            port += 1

    start_time = time.time()

    for process in processes:
        process.join()

if __name__ == '__main__':

    if (len( sys.argv ) > 1):
        print("Removing old transactions data and generating new data for transaction processing ....")
        Bootstrap.run()
        # clear report
        if((len(sys.argv) >2) and sys.argv[2]=="clear-report"):
            clear_reports()
    else:
        if(os.path.exists(os.path.abspath(os.curdir)+'/storages')):
            print('Start processing existing transactions!')
        else:
            print('Transactions and storages not found please run the command with extra parameters like: "python3 main.py storage"')
            exit()

    parallel_transactions_processing()
    generate_report(start_time)


