
import datetime
import os
import time
from config import *
import numpy as np 
import matplotlib.pyplot as plt
from utility.file import File
import pandas as pd

from utility.shard import get_number_of_leader_shards 

def generate_report(start_time):
    generate_general_report(start_time)

def generate_general_report(start_time):
    total_time = "{}".format(time.time() - start_time)
    print("\n")
    print("------------------------------ REPORT STARTING ... ------------------------------")
    print("Total Number of Shards : " +str(len(SHARDS))+'\n'
        + "Total Number of Leader Shards : "+str(get_number_of_leader_shards())+'\n'
        + "Total Number of Transaction : " +str(TOTAL_NUMBER_OF_TRANSACTIONS)+'\n'
        + "Total Number of Subtransactions : " +str(TOTAL_NUMBER_OF_TRANSACTIONS*(NUMBER_OF_CONDITIONS+2))+'\n'
        + "Total Number of Accounts : " +str(NUMBER_OF_ACCOUNTS)+'\n'
        + "Total Conditions Per Transaction : " +str((NUMBER_OF_CONDITIONS+2))+'\n'
        + "Total Processing Time in Seconds : "+ str(total_time)+'\n'
        + "Transaction Per Second (Throughput) : "+ str(int(TOTAL_NUMBER_OF_TRANSACTIONS)/float(total_time))
    )
    write_report_to_file(total_time)
    print("------------------------------ REPORT END !!! ---------------------------------")
    print("\n")
    

def write_report_to_file(total_processong_time):
    report_header = ['NUMBER_OF_SHARDS', 'NUMBER_OF_TRANSACTION', 'NUMBER_OF_SUBTRANSACTION', 'NUMBER_OF_ACCOUNTS', 'NUMBER_OF_CONDITION_PER_TRANSACTION','TOTAL_PROCESSING_TIME','TRANSACTION_PER_SECONDS', 'TIMESTAMP', 'TRANSACTION_TYPE']
    log_file_path = '/reports/report.csv'
    existing_data = File.open_file(log_file_path)
    if(existing_data==None):
         File.write_file(log_file_path,report_header)
    data = [len(SHARDS), TOTAL_NUMBER_OF_TRANSACTIONS, TOTAL_NUMBER_OF_TRANSACTIONS*(NUMBER_OF_CONDITIONS+2), NUMBER_OF_ACCOUNTS, (NUMBER_OF_CONDITIONS+2), total_processong_time, int(TOTAL_NUMBER_OF_TRANSACTIONS)/float(total_processong_time), datetime.datetime.now(),TRANSACTION_TYPE]
   
    File.append_data(log_file_path,data)

def number_of_shard_vs_tsp():
    # print('report rapid chain fig4')
    data = get_data_from_file()
    type='chart'
    x_axis = [1, 2, 3, 4, 5]
    x_axis = list(data['NUMBER_OF_SHARDS'])
    # lock_based = [10,2,20,40,500]
    # optimal = [5,30,25,30,60]
    # our_protocol = [30,30,35,30,70]
    our_protocol = list(data['TRANSACTION_PER_SECONDS'])
    lock_based = list(data['TRANSACTION_PER_SECONDS'])
    optimal = list(data['TOTAL_PROCESSING_TIME'])
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

# number_of_shard_vs_latency()
def get_data_from_file():
    csv_file = os.path.abspath(os.curdir)+'/reports/report.csv'
    return pd.read_csv(csv_file)


def clear_reports():
    File.remove_all_file_inside_directory('/reports')
    print('All report cleared!')

def clear_log_files():
    File.remove_all_file_inside_directory('/logs')
    
# number_of_shard_vs_tsp()