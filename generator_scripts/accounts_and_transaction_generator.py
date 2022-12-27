import csv
import datetime
import hashlib
import multiprocessing
import os
import sys
import string
import random

from utility.shard import get_shard_for_account

sys.path.append(os.path.abspath(os.curdir))
from  config import *
from utility.file import File

class AccountsAndTransactionGenerator:
    
   
    def create_tmp_accounts_file():
        # this function create random accounts and save that accounts to file
        tmp_account_save_file_path = '/storages/GENERATED_ACCOUNTS.CSV'
        
        for na in range(NUMBER_OF_ACCOUNTS):
                name = ''.join(random.choices(string.ascii_uppercase, k=3))
                data = [str(na)+'_'+name, name, DEFAULT_AMOUNT]
                File.append_data(tmp_account_save_file_path,data)
    

   
    def assign_accounts_to_shard():
         # this function pick the accounts from tmp accounts file and assign to that accounts to shards
        tmp_account_file_path ='/storages/GENERATED_ACCOUNTS.CSV'
        dataReader = File.open_file(tmp_account_file_path)
       
        i=0
        for account in dataReader:
            account_number = account[ACCOUNT_INDEX_ACCOUNT_NUMBER]
            
            # skip header of csv file
            if(account_number=='ACCOUNT_NUMBER'):
                continue
            
            shard_id = get_shard_for_account(account_number)
            assign_account_to_shard_file_path = '/storages/shards/'+str(shard_id)+'/transactions/committed/'+TRANSACTION_FILE_NAME
            acc_generator = AccountsAndTransactionGenerator()
            data = acc_generator.get_account_row_data(account)
            File.append_data(assign_account_to_shard_file_path, data)

    
    def get_account_row_data(self,account):
        data = [   
                    'TXN_'+hashlib.sha256((str(datetime.datetime.now())+account[ACCOUNT_INDEX_ACCOUNT_NUMBER]).encode()).hexdigest(),
                    'SUB_TXN_'+hashlib.sha256((str(datetime.datetime.now())+account[ACCOUNT_INDEX_ACCOUNT_NUMBER]+account[ACCOUNT_INDEX_ACCOUNT_NAME]).encode()).hexdigest(),
                    account[ACCOUNT_INDEX_ACCOUNT_NUMBER],
                    account[ACCOUNT_INDEX_ACCOUNT_NAME],
                    account[ACCOUNT_INDEX_AMOUNT],
                    datetime.datetime.now()
                ]
        return data
                
    
    
    def generate_and_assign_transaction_parallely(self, shard_id):
        # this function create the transaction using accounts in tmp accounts file
        # and append this created transaction to transactionpool
        tmp_account_save_file_path = '/storages/GENERATED_ACCOUNTS.CSV'
        for nt in range(NUMBER_OF_TRANSACTIONS_IN_EACH_TRANSACTION_POOL):
                # print("Generated transaction pool for shard: "+str(shard_id))
                data = File.open_file(tmp_account_save_file_path)
                random_upper_bound=NUMBER_OF_ACCOUNTS-1
                from_row = data[random.randint(1,random_upper_bound)]
                to_row = data[random.randint(1,random_upper_bound)]
                conditions=''
                for con in range(NUMBER_OF_CONDITIONS):
                    single_account = data[random.randint(1,random_upper_bound)]
                    if con!=(NUMBER_OF_CONDITIONS-1):
                        conditions+=single_account[ACCOUNT_INDEX_ACCOUNT_NUMBER]+CONDITION_HAS+single_account[ACCOUNT_INDEX_AMOUNT]+CONDITION_AND
                    else:
                        conditions+=single_account[ACCOUNT_INDEX_ACCOUNT_NUMBER]+CONDITION_HAS+single_account[ACCOUNT_INDEX_AMOUNT]
                data = ['TXN_'+hashlib.sha256((str(datetime.datetime.now())+single_account[ACCOUNT_INDEX_ACCOUNT_NUMBER]).encode()).hexdigest(),from_row[ACCOUNT_INDEX_ACCOUNT_NUMBER], to_row[ACCOUNT_INDEX_ACCOUNT_NUMBER],from_row[ACCOUNT_INDEX_AMOUNT],conditions,datetime.datetime.now()]
                txn_pool_file_name = '/storages/shards/'+str(shard_id)+'/transactions/pools/initial/'+TRANSACTION_FILE_NAME
                File.append_data(txn_pool_file_name, data)
    
 
   
    

    def create_transaction_and_append_to_transaction_pool():
        process_list = []
        for shard in SHARDS:
            if(shard[1]):
                p =  multiprocessing.Process(
                    target= AccountsAndTransactionGenerator().generate_and_assign_transaction_parallely,
                    args=(shard[0],)
                    )
                p.start()
                process_list.append(p)

        for process in process_list:
            process.join()