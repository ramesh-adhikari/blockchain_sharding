import multiprocessing
from client import init_client
from models.shard import Shard
from server import init_server
from shard import *
from config import INITAIL_PORT


processes = []

def init_process(server,shard_id, port):
    p = multiprocessing.Process(target=init_server if server else init_client, args=(shard_id,port,))
    processes.append(p)
    p.start()

def init_clients(shards,port):
    for shard in shards:
        init_process(False,shard.id,port) #client

if __name__ == '__main__':
    shards = generate_shards()
    port = INITAIL_PORT
    for shard in shards:
        if(shard.is_leader):
            init_process(True,shard.id,port) #server
            init_clients(shards,port)
            port += 1

    for process in processes:
        process.join()

# LS1, S2, S3, LS4
# LS1 (8085), LS4 (8086)
# S2(8085), S2(8086)
# S3(8085), S2(8086)

#S2->A , S3->B
#A->LS1->S2->8085, A->LS4->S2->8086

# Leader LS1, LS4
#LS1 -> S1, S2, S3, S4
#LS4 -> S1, S2, S3, S4

#TODO Ramesh move this to main, add flag to genrate data 
#TODO Ramesh print basic log after completion, number of shards : , leader shard : , transactions ,total time : , accounts ,Conditions : , Total subtransactions