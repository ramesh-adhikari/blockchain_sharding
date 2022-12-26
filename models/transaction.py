import datetime
import os
import sys
import pandas as pd
sys.path.append(os.path.abspath(os.curdir))
from config import *
from utility.shard import get_shard_for_account
from utility.file import File


class Transaction:
    
    def __init__(self, txn_id, sender, receiver, amount, type):
        self.txn_id = txn_id
        self.sender = sender
        self.receiver = receiver
        self.amount = amount
        self.type = type #pending transaction or commiting transaction
        
        
    def append_temporary_transaction(account_number, account_name, txn_id, sub_txn_id, amount):
        shard_id = get_shard_for_account(account_number)
        shard_file_name = SHARD_NAME_PREFIX+str(shard_id)+'.CSV'
        shard_file_path = '/storages/transactions/temporary/'+shard_file_name
        timestamp = datetime.datetime.now()
        data = [txn_id, sub_txn_id, account_number, account_name, amount, timestamp]
        File.append_data(shard_file_path, data)
        
        
    def commit_transaction(account_number, transaction_id):
        # move pending transaction to confirm transaction
        print('move pending transaction to confirm transaction')
        shard_id = get_shard_for_account(account_number)
        shard_file_name = SHARD_NAME_PREFIX+str(shard_id)+'.CSV'
        shard_file_path = os.path.abspath(os.curdir)+'/storages/transactions/temporary/'+shard_file_name
        transaction = pd.read_csv(shard_file_path)
        selected_rows = transaction.loc[transaction['TXN_ID'] == transaction_id]
        move_data = [selected_rows['TXN_ID'][0]
                     ,selected_rows['SUB_TXN_ID'][0],
                     selected_rows['ACCOUNT_NUMBER'][0],
                     selected_rows['ACCOUNT_NAME'][0],
                     selected_rows['AMOUNT'][0],
                     selected_rows['TIMESTAMP'][0]
                     ]
        # copy data from temporary to confirm
        dest_shard_file_name = SHARD_NAME_PREFIX+str(shard_id)+'.CSV'
        dest_shard_file_path = '/storages/transactions/confirmed/'+dest_shard_file_name
        File.append_data(dest_shard_file_path,move_data)
        
        # delete row from temporary
        delete_row = pd.read_csv(shard_file_path,index_col ="TXN_ID")
       
        delete_row.drop([selected_rows['TXN_ID'][0]],inplace=True)
        delete_row.to_csv(shard_file_path) 
        
    
    def get_transactions_from_transaction_pool():
        print('return transaction from transaction pull')
    
    def set_transaction_to_temporary_pool():
        print('set transaction')
    
    def move_transaction_from_temporary_pool_to_abort():
        print('move')
    
    def move_transaction_from_temporary_to_initial():
        print('move to intital')
    
    def remove_transaction_from_temporary_pool():
        print('remove')
        
    
    def has_amount(account_number, amount):
        # check whether the account has sufficient balance or not
        shard_id = get_shard_for_account(account_number)
        shard_file_name = SHARD_NAME_PREFIX+str(shard_id)+'.CSV'
        shard_file_path = '/storages/transactions/confirmed/'+shard_file_name
        shard_file_directory = os.path.abspath(os.curdir)+shard_file_path
        data_frame = pd.read_csv(shard_file_directory)
        # sum the amount associated with given account number
        account_amount = data_frame.loc[data_frame['ACCOUNT_NUMBER'] == account_number, 'AMOUNT'].sum()
        if(account_amount>=amount):
            return True
        return False    