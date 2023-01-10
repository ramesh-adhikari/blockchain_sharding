
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
        + "Transaction Per Second (Throughput) : "+ str(int(TOTAL_NUMBER_OF_TRANSACTIONS)/float(total_time))+'\n'
         + "Duration of execution : "+ str(float(total_time)/int(TOTAL_NUMBER_OF_TRANSACTIONS))
    )
    write_report_to_file(total_time)
    print("------------------------------ REPORT END !!! ---------------------------------")
    print("\n")
    

def write_report_to_file(total_processong_time):
    report_header = ['NUMBER_OF_SHARDS', 'NUMBER_OF_TRANSACTION', 'NUMBER_OF_SUBTRANSACTION', 'NUMBER_OF_ACCOUNTS', 'NUMBER_OF_CONDITION_PER_TRANSACTION','TOTAL_PROCESSING_TIME','TRANSACTION_PER_SECONDS', 'TIMESTAMP', 'DURATION_OF_EXECUTION', 'TRANSACTION_TYPE']
    log_file_path = '/reports/report.csv'
    existing_data = File.open_file(log_file_path)
    if(existing_data==None):
         File.write_file(log_file_path,report_header)
    data = [len(SHARDS), TOTAL_NUMBER_OF_TRANSACTIONS, TOTAL_NUMBER_OF_TRANSACTIONS*(NUMBER_OF_CONDITIONS+2), NUMBER_OF_ACCOUNTS, (NUMBER_OF_CONDITIONS+2), total_processong_time, int(TOTAL_NUMBER_OF_TRANSACTIONS)/float(total_processong_time), datetime.datetime.now(),float(total_processong_time)/int(TOTAL_NUMBER_OF_TRANSACTIONS),TRANSACTION_TYPE]
   
    File.append_data(log_file_path,data)

def number_of_shard_vs_tsp():
    data = get_data_from_file("NUMBER_OF_SHARDS","TRANSACTION_PER_SECONDS")
    type='lines'
    print(data)
    x_label='Number of shards'
    y_label='Throughput (TPS)'
    plot_show(type, data[0], data[1], data[2], data[3], x_label, y_label)

def number_of_condition_vs_execution_time():
    data = get_data_from_file("NUMBER_OF_CONDITION_PER_TRANSACTION","DURATION_OF_EXECUTION")
    type='chart'
    x_label='Number of Constraints (conditions)'
    y_label='Execution Time of a Transaction (sec)'
    plot_show(type, data[0], data[1], data[2], data[3], x_label, y_label)


def plot_show(type, x_axis, optimal, lock_based, our_protocol, x_label, y_label):

    if(type=='lines'):
        plt.plot(x_axis, optimal, label = 'optimal (no lock)', linestyle="-.",color = '#b3b3b3')
        plt.plot(x_axis, our_protocol, label = 'our protocol', linestyle=":",color = '#2770C0')
        plt.plot(x_axis, lock_based, label = 'lock based', linestyle="-", color = '#FC281A')
    else:
        # draw chart
        x_axis = np.arange(5,25,5)
        print(x_axis+1)
       
        plt.bar(x_axis -1, optimal, 1,label = 'optimal (no lock)',color = '#b3b3b3')
        plt.bar(x_axis+0, our_protocol,1,  label = 'our protocol',color = '#2770C0')
        plt.bar(x_axis+1, lock_based, 1,label = 'lock based',color = '#FC281A')
        
    
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.legend()
    plt.show()

# number_of_shard_vs_latency()
def get_data_from_file(indexing_param, resturn_value_index):
    print('here')
    csv_file = os.path.abspath(os.curdir)+'/reports/report.csv'
    data =  pd.read_csv(csv_file)
    number_of_unique_shards = data.drop_duplicates(indexing_param)
    number_of_shards = list(number_of_unique_shards[indexing_param])
    
    optimal = []
    lock_based = []
    our_protocol = []
    for row in number_of_shards:
        selected_optimal_row = data.loc[(data[indexing_param] == row) & (data["TRANSACTION_TYPE"]=='NO_LOCK')].tail(1)
        selected_lock_based_row = data.loc[(data[indexing_param] == row) & (data["TRANSACTION_TYPE"]=='LOCK')].tail(1)
        selected_our_protocol_row = data.loc[(data[indexing_param] == row) & (data["TRANSACTION_TYPE"]=='OUR_PROTOCOL')].tail(1)
        selected_optimal_row = data.loc[(data[indexing_param] == row) & (data["TRANSACTION_TYPE"]=='NO_LOCK')].tail(1)
        if(len(selected_optimal_row)>0):
            optimal.append(selected_optimal_row[resturn_value_index] [selected_optimal_row.index[0]])
        else:
            optimal.append(0)
        if(len(selected_lock_based_row)>0):
            lock_based.append(selected_lock_based_row[resturn_value_index][selected_lock_based_row.index[0]])
        else:
            lock_based.append(0)
        if(len(selected_our_protocol_row)>0):
            our_protocol.append(selected_our_protocol_row[resturn_value_index] [selected_our_protocol_row.index[0]])
        else:
            our_protocol.append(0)
       
    return  number_of_shards, optimal, lock_based, our_protocol
    


def clear_reports():
    File.remove_all_file_inside_directory('/reports')
    print('All report cleared!')

def clear_log_files():
    File.remove_all_file_inside_directory('/logs')
    
number_of_condition_vs_execution_time()