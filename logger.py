
import time
from config import *
import numpy as np 
import matplotlib.pyplot as plt 

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

def number_of_shard_vs_tsp():
    # print('report rapid chain fig4')
    type='chart'
    x_axis = [1, 2, 3, 4, 5]
    lock_based = [10,2,20,40,500]
    optimal = [5,30,25,30,60]
    our_protocol = [30,30,35,30,70]
    x_label='Number of shards'
    y_label='Throughput (TPS)'
    plot_show(type, x_axis, lock_based, optimal, our_protocol, x_label, y_label)

def number_of_condition_vs_tsp():
    type='chart'
    x_axis = [1, 2, 3, 4, 5]
    lock_based = [10,2,20,40,50]
    optimal = [5,30,25,30,60]
    our_protocol = [30,30,35,30,70]
    x_label='Number of Conditions'
    y_label='Throughput (TPS)'
    plot_show(type, x_axis, lock_based, optimal, our_protocol, x_label, y_label)

def number_of_shard_vs_latency():
    # print('report rapid chain fig3')
    type='lines'
    x_axis = [1, 2, 3, 4, 5]
    lock_based = [10,2,20,40,50]
    optimal = [5,30,25,30,60]
    our_protocol = [30,30,35,30,70]
    x_label='Number of shards'
    y_label='Latency'
    plot_show(type, x_axis, lock_based, optimal, our_protocol, x_label, y_label)



def plot_show(type, x_axis, lock_based, optimal, our_protocol, x_label, y_label):

    if(type=='lines'):
        plt.plot(x_axis, lock_based, label = 'lock based', linestyle="-", color = '#FC281A')
        plt.plot(x_axis, optimal, label = 'optimal (no lock)', linestyle="-.",color = '#b3b3b3')
        plt.plot(x_axis, our_protocol, label = 'our protocol', linestyle=":",color = '#2770C0')
    else:
        # draw chart
        x_axis = np.arange(1,(len(x_axis)+1))
        plt.bar(x_axis - 0.2, lock_based, 0.2, label = 'lock based',color = '#FC281A')
        plt.bar(x_axis + 0.0, optimal, 0.2, label = 'optimal (no lock)',color = '#b3b3b3')
        plt.bar(x_axis + 0.2    , our_protocol, 0.2, label = 'our protocol',color = '#2770C0')
    
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.legend()
    plt.show()

number_of_condition_vs_tsp()

