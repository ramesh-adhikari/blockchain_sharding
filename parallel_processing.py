import multiprocessing
from client import init_client
from config import NUMBER_OF_SHARDS
from models.shard import Shard
from server import init_server
from shard import *


def init_shard(shard: Shard):
    if (shard.is_leader):
        init_server(shard.id)
    else:
        init_client(shard.id)


if __name__ == '__main__':

    processes = []
    for shard in prepare_client_server_shards():
        p = multiprocessing.Process(target=init_shard, args=(shard,))
        processes.append(p)
        p.start()

    for process in processes:
        process.join()
