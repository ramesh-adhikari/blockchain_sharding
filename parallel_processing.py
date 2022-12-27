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
        init_process(False,shard.id,port)

if __name__ == '__main__':
    shards = generate_shards()
    port = INITAIL_PORT
    for shard in shards:
        if(shard.is_leader):
            init_process(True,shard.id,port)
            init_clients(shards,port)
            port += 1

    for process in processes:
        process.join()


