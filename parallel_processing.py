import multiprocessing
from client import init_client
from config import NUMBER_OF_SHARDS
from server import init_server


def init_shard(i):
    print("Init shard"+str(i))
    if (i == 0):  # leader shard
        init_server(i)
    else:
        init_client(i)


if __name__ == '__main__':

    processes = []
    for i in range(0, NUMBER_OF_SHARDS):
        p = multiprocessing.Process(target=init_shard, args=(i,))
        processes.append(p)
        p.start()

    for process in processes:
        process.join()
