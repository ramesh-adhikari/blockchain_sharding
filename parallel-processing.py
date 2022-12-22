import multiprocessing
from client import init_client
from config import NUMBER_OF_SHARDS
from server import init_server


def init_shard(x):
    if (x == 0):  # leader shard
        init_server()
    else:
        init_client(x)


if __name__ == '__main__':
    processes = []
    for i in range(0, NUMBER_OF_SHARDS):
        p = multiprocessing.Process(target=init_shard, args=(i,))
        processes.append(p)
        p.start()

    for process in processes:
        process.join()
