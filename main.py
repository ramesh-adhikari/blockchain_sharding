import os
import time
from generator_scripts.bootstrap import Bootstrap
from report import clear_log_files, clear_reports, generate_report
import sys
import multiprocessing
from client import init_client
from server import init_server
from config import *

processes = []
leaders = {}
start_time = 0


def init_shards():
    global start_time, leaders
    leaders.clear()
    port = INITAIL_PORT
    for shard in SHARDS:
        if (shard[1]):  # is_leader
            p = multiprocessing.Process(
                target=init_server,
                args=(shard[0], port)
            )
            processes.append(p)
            p.start()
            leaders[str(shard[0])] = port
            port += 1

    for shard in SHARDS:
        p = multiprocessing.Process(
            target=init_client,
            args=(shard[0], leaders)
        )
        processes.append(p)
        p.start()

    start_time = time.time()

    for process in processes:
        process.join()


if __name__ == '__main__':
    if (len(sys.argv) > 1):
        print("Removing old transactions data and generating new data for transaction processing ....")
        Bootstrap.run()
        # clear report
        if ((len(sys.argv) > 2) and sys.argv[2] == "clear-report"):
            clear_reports()
    else:
        if (os.path.exists(os.path.abspath(os.curdir)+'/storages')):
            print('Start processing existing transactions!')
        else:
            print('Transactions and storages not found please run the command with extra parameters like: "python3 main.py storage"')
            exit()

    if (WRITE_LOG_TO_FILE):
        if not os.path.exists('logs'):
            os.makedirs('logs')
        clear_log_files()
        print("Processing transactions ...")
        print(
            "Please go through 'logs' directory for output after transaction is processed.")
        print("Disable 'WRITE_LOG_TO_FILE' flag on config file to print output to console instead of file.")

    init_shards()
    generate_report(start_time)
