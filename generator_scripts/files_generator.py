import sys
import os

sys.path.append(os.path.abspath(os.curdir))
from config import *
from utility.file import File

class FilesGenerator:
    
    # This function create the n (n= number of shards) temporary and confirmed files to store the transaction
    def create_shard_transaction_file():
        temp_file_path = '/storages/transactions/temporary/'
        confirm_file_path = '/storages/transactions/confirmed/'
        transaction_header = ['TXN_ID', 'SUB_TXN_ID', 'ACCOUNT_NUMBER', 'ACCOUNT_NAME', 'AMOUNT', 'TIMESTAMP']
        
        for n_shard in range(NUMBER_OF_SHARDS):
            temp_file_name = temp_file_path+TEMPORARY_SHARD_NAME_PREFIX+str(n_shard)+'.CSV'
            confirm_file_name = confirm_file_path+CONFIRMED_SHARD_NAME_PREFIX+str(n_shard)+'.CSV'
            File.write_file(temp_file_name,transaction_header)
            File.write_file(confirm_file_name,transaction_header)
    
    def create_tmp_account_file():
        tmp_account_save_file_path = '/storages/GENERATED_ACCOUNTS.CSV'
        account_header = ['ACCOUNT_NUMBER', 'ACCOUNT_NAME', 'AMOUNT']
        File.write_file(tmp_account_save_file_path, account_header)
    
    def create_transaction_pool_file():
        txn_pool_path = '/storages/transaction_pools/'
        transaction_header = ['TXN_ID', 'SENDER_ACCOUNT_NUMBER', 'RECEIVER_ACCOUNT_NUMBER', 'AMOUNT', 'CONDITIONS', 'TIMESTAMP']
        for n_leader in range(NUMBER_OF_LEADER_SHARD):
            txn_pool_file_name = txn_pool_path+TRANSACTION_POOL_FOR_LEADER_NAME_PREFIX+str(n_leader)+'.CSV'
            File.write_file(txn_pool_file_name,transaction_header)

    
        
    def remove_generated_files():
        File.remove_all_file_inside_directory('/storages/transactions/confirmed/')
        File.remove_all_file_inside_directory('/storages/transactions/temporary/')
        File.remove_all_file_inside_directory('/storages/transaction_pools/')
        File.remove_file('/storages/GENERATED_ACCOUNTS.CSV')
    
    def remove_tmp_accounts_file():
        # remove the tmp accounts file
        tmp_account_save_file_path = '/storages/GENERATED_ACCOUNTS.CSV'
        File.remove_file(tmp_account_save_file_path)