
import time
from generator_scripts.bootstrap import Bootstrap
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

    starttime = time.time()

    if (len( sys.argv ) > 1):
        print("Erase old transactions and generate new transactions and process these transactions")
        Bootstrap.run()
        parallel_transactions_processing()
    else:
        print('Start processing existing transactions!')
        parallel_transactions_processing()
    
    total_number_of_leader=0
    for shard in SHARDS:
        if(shard[1]):
            total_number_of_leader = total_number_of_leader+1

    total_number_of_transaction = NUMBER_OF_TRANSACTIONS_IN_EACH_TRANSACTION_POOL*total_number_of_leader
    print("Total Number of Shards : " +str(len(SHARDS)) 
        + " Total Number of Leader Shards : "+str(total_number_of_leader)
        + " Total Transaction : " +str(total_number_of_transaction)
        + " Total Number of Subtransactions : " +str(total_number_of_transaction*(NUMBER_OF_CONDITIONS+1))
        + " Total Accounts : " +str(NUMBER_OF_ACCOUNTS)
        + " Conditions per transactions : " +str((NUMBER_OF_CONDITIONS+1))
    )
    print("To process total transaction with it took {} seconds".format(time.time() - starttime))

