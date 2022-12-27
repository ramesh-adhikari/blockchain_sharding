import csv
import datetime
import os
import sys
import pandas as pd
sys.path.append(os.path.abspath(os.curdir))
from config import *
from utility.shard import get_shard_for_account
from utility.file import File


class Transaction:
    
    # def __init__(self, txn_id, sender, receiver, amount, type):
    #     self.txn_id = txn_id
    #     self.sender = sender
    #     self.receiver = receiver
    #     self.amount = amount
    #     self.type = type #pending transaction or commiting transaction
        
        # shard_id, sub_txn_id
    def append_temporary_transaction(account_number, account_name, txn_id, sub_txn_id, amount):
        shard_id = get_shard_for_account(account_number)
        shard_file_path = '/storages/shards/'+str(shard_id)+'/transactions/temporary/'+TRANSACTION_FILE_NAME
        timestamp = datetime.datetime.now()
        data = [txn_id, sub_txn_id, account_number, account_name, amount, timestamp]
        File.append_data(shard_file_path, data)
        
        
    def commit_transaction(shard_id, txn_id):
        # move pending transaction to confirm transaction
        print('move pending transaction to confirm transaction')
        source_file = '/storages/shards/'+str(shard_id)+'/transactions/temporary/'+TRANSACTION_FILE_NAME
        destination_file = '/storages/shards/'+str(shard_id)+'/transactions/confirmed/'+TRANSACTION_FILE_NAME
        transaction = Transaction()
        transaction.move_transaction(source_file, destination_file, txn_id, 'TRANSACTION')
        
    
    def get_transactions_from_transaction_pool(shard_id):
        txn_pool_initial_file_name = os.path.abspath(os.curdir)+'/storages/shards/'+str(shard_id)+'/transactions/pools/initial/'+TRANSACTION_FILE_NAME
       
        data = pd.read_csv(txn_pool_initial_file_name)
        # sort transaction as per timestamp
        data['TIMESTAMP'] = pd.to_datetime(data['TIMESTAMP'], format="%Y/%m/%d %H:%M")
        data = data.sort_values(by='TIMESTAMP', ascending=True)
        data.to_csv(txn_pool_initial_file_name,index=False)
        # print(data.to_csv(index=False))

        with open(txn_pool_initial_file_name, newline='') as f:
            reader = csv.reader(f)
            row1 = next(reader)
            row2=next(reader)
        return row2
    
    def move_transaction_from_initial_to_temporary_pool(shard_id, txn_id):
        source_file = '/storages/shards/'+str(shard_id)+'/transactions/pools/initial/'+TRANSACTION_FILE_NAME
        
        destination_file = '/storages/shards/'+str(shard_id)+'/transactions/pools/temporary/'+TRANSACTION_FILE_NAME
        transaction = Transaction()
        transaction.move_transaction(source_file, destination_file, txn_id)
        
    def move_transaction_from_temporary_to_abort_pool(shard_id, txn_id):
        source_file = '/storages/shards/'+str(shard_id)+'/transactions/pools/temporary/'+TRANSACTION_FILE_NAME
        destination_file = '/storages/shards/'+str(shard_id)+'/transactions/pools/aborted/'+TRANSACTION_FILE_NAME
        transaction = Transaction()
        transaction.move_transaction(source_file, destination_file, txn_id)
    
    def move_transaction_from_temporary_to_initial(shard_id, txn_id):
        source_file = '/storages/shards/'+str(shard_id)+'/transactions/pools/temporary/'+TRANSACTION_FILE_NAME
        destination_file = '/storages/shards/'+str(shard_id)+'/transactions/pools/initial/'+TRANSACTION_FILE_NAME
        transaction = Transaction()
        transaction.move_transaction(source_file, destination_file, txn_id)
    
    def remove_transaction_from_temporary_pool(shard_id, txn_id):
        temporary_pool_txn_path = '/storages/shards/'+str(shard_id)+'/transactions/pools/temporary/'+TRANSACTION_FILE_NAME
        t_instance = Transaction()
        t_instance.delete_row_by_txn_id(temporary_pool_txn_path,txn_id)
        
    
    def has_amount(account_number, amount):
        # check whether the account has sufficient balance or not
        shard_id = get_shard_for_account(account_number)
        shard_file_path = '/storages/shards/'+str(shard_id)+'/transactions/confirmed/'+TRANSACTION_FILE_NAME
        shard_file_directory = os.path.abspath(os.curdir)+shard_file_path
        data_frame = pd.read_csv(shard_file_directory)
        # sum the amount associated with given account number
        account_amount = data_frame.loc[data_frame['ACCOUNT_NUMBER'] == account_number, 'AMOUNT'].sum()
        if(account_amount>=amount):
            return True
        return False    
    
    def move_transaction(self, source_file, destination_file, txn_id, type='TRANSACTION_POOL'):
        source_file = source_file
        destination_file = destination_file
        
        transaction = pd.read_csv(os.path.abspath(os.curdir)+source_file)
        selected_rows = transaction.loc[transaction['TXN_ID'] == txn_id]
        t_instance = Transaction()
        move_data = t_instance.get_move_data(selected_rows, type)
        File.append_data(destination_file,move_data)
        
        # delete row from temporary
        t_instance.delete_row_by_txn_id(source_file,selected_rows['TXN_ID'][0])
        
    def get_move_data(self, row, type):
        if(type=='TRANSACTION_POOL'):
           return [row['TXN_ID'][0],   
                     row['SENDER_ACCOUNT_NUMBER'][0],
                     row['RECEIVER_ACCOUNT_NUMBER'][0],
                     row['AMOUNT'][0],
                     row['CONDITIONS'][0],
                     row['TIMESTAMP'][0]
                    ] 
        elif(type=='TRANSACTION'):
            return [row['TXN_ID'][0],
                     row['SUB_TXN_ID'][0],
                     row['ACCOUNT_NUMBER'][0],
                     row['ACCOUNT_NAME'][0],
                     row['AMOUNT'][0],
                     row['TIMESTAMP'][0]
                     ]
    
    def delete_row_by_txn_id(self, file_path, txn_id):
        abs_file_path = os.path.abspath(os.curdir)+file_path
        transaction = pd.read_csv(abs_file_path)
        transaction.drop(transaction.index[(transaction["TXN_ID"] == txn_id)],axis=0,inplace=True)
        transaction.to_csv(abs_file_path,index=False)