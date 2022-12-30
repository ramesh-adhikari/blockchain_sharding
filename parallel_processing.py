import multiprocessing
from client import init_client
from server import init_server
from config import INITAIL_PORT, SHARDS


processes = []

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

if __name__ == '__main__':
    port = INITAIL_PORT
    for shard in SHARDS:
        if(shard[1]): # is leader
            init_process(True,shard[0],port,shard[0]) #server
            init_clients(port,shard[0])
            port += 1

    for process in processes:
        process.join()