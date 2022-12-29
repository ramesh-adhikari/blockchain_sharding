import multiprocessing
from client import init_client
from server import init_server
from config import INITAIL_PORT, SHARDS


processes = []

def init_process(server,shard_id, port):
    p = multiprocessing.Process(target=init_server if server else init_client, args=(shard_id,port,))
    processes.append(p)
    p.start()

def init_clients(port):
    for shard in SHARDS:
        init_process(False,shard[0],port) #client

if __name__ == '__main__':
    port = INITAIL_PORT
    for shard in SHARDS:
        if(shard[1]): # is leader
            init_process(True,shard[0],port) #server
            init_clients(port)
            port += 1

    for process in processes:
        process.join()