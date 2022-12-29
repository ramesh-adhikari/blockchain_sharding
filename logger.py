
import time
from config import *

def generate_log(start_time):
    generate_report(start_time)

def generate_report(start_time):
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
    print("To process total transaction with it took {} seconds".format(time.time() - start_time))