
import csv
import datetime
import os
import sys
import string
import random
import hashlib
import pandas as pd
from config import *


class TransactionUtility:
         
    def check_condition(self):
        return print('this is condition check')
    
    def is_account_has_sufficient_amount(account_number, amount):
        utility = TransactionUtility()
        shard_file_name = utility.get_associated_shard_file_name_from_account_number(account_number)
        shard_file_directory = os.path.abspath(os.curdir)+'/datas/'+shard_file_name
        data_frame = pd.read_csv(shard_file_directory)
        account_amount = data_frame.loc[data_frame['ACCOUNT_NUMBER'] == account_number, 'AMOUNT'].sum()
        if(account_amount>=amount):
            return True
        return False
        
        

    def set_version_of_account(account_number, version):
        return print("this will set the version of account")
    
    def get_version_of_account(account_number):
        return print('this will return the current version of account')
    
    def assign_account_to_shards(self, account):
        account_number = account[ACCOUNT_INDEX_ACCOUNT_NUMBER]
        utility = TransactionUtility()
        shard_file_name = utility.get_associated_shard_file_name_from_account_number(account_number)
        
        assign_account_to_shard_file_path = os.path.abspath(os.curdir)+'/datas/'+shard_file_name
        account_header = ['ACCOUNT_NUMBER', 'ACCOUNT_NAME', 'AMOUNT','TXN_ID', 'TIMESTAMP']
        with open(assign_account_to_shard_file_path, 'a') as file:
                
                # check if there exist header in file or not 
                # if header not exist in csv add header
               with open(assign_account_to_shard_file_path, 'r') as shard_account:
                dataReader = csv.reader(shard_account)
                
                if(len(list(dataReader))==0):
                    writer = csv.writer(file)
                    writer.writerow(account_header)
                # append account info to shard
                timestamp = datetime.datetime.now()
                transactionId = hashlib.sha256(str(timestamp).encode()).hexdigest()
                account.append(transactionId)
                account.append(timestamp)
                writer = csv.writer(file)
                writer.writerow(account)
    
    def assign_all_account_to_each_shards():
        account_save_file_path = os.path.abspath(os.curdir)+'/datas/GENERATED_ACCOUNTS.CSV'
        utility = TransactionUtility()
        with open(account_save_file_path, 'r') as account:
                dataReader = csv.reader(account)
                i=0
                for row in dataReader:
                    if(i!=0):
                        utility.assign_account_to_shards(row)
                    i=i+1
    
    def get_associated_shard_file_name_from_account_number(self, account_number):
        account_id = account_number.split("_")[0]
        shard_id = int(account_id)%NUMBER_OF_SHARDS
        return SHARD_NAME_PREFIX+str(shard_id)+'.CSV'
        
                
                
        
        
        