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
            temp_file_name = temp_file_path+SHARD_NAME_PREFIX+str(n_shard)+'.CSV'
            confirm_file_name = confirm_file_path+SHARD_NAME_PREFIX+str(n_shard)+'.CSV'
            File.write_file(temp_file_name,transaction_header)
            File.write_file(confirm_file_name,transaction_header)
    
    def create_tmp_account_file():
        tmp_account_save_file_path = '/storages/GENERATED_ACCOUNTS.CSV'
        account_header = ['ACCOUNT_NUMBER', 'ACCOUNT_NAME', 'AMOUNT']
        File.write_file(tmp_account_save_file_path, account_header)
    
    def create_transaction_pool_file():
        txn_pool_initial_path = '/storages/transaction_pools/initial/'
        txn_pool_temporary_path = '/storages/transaction_pools/temporary/'
        txn_pool_aborted_path = '/storages/transaction_pools/aborted/'
        transaction_header = ['TXN_ID', 'SENDER_ACCOUNT_NUMBER', 'RECEIVER_ACCOUNT_NUMBER', 'AMOUNT', 'CONDITIONS', 'TIMESTAMP']
        for n_leader in range(NUMBER_OF_LEADER_SHARD):
            txn_pool_initial_file_name = txn_pool_initial_path+SHARD_NAME_PREFIX+str(n_leader)+'.CSV'
            txn_pool_temporary_file_name = txn_pool_temporary_path+SHARD_NAME_PREFIX+str(n_leader)+'.CSV'
            txn_pool_aborted_file_name = txn_pool_aborted_path+SHARD_NAME_PREFIX+str(n_leader)+'.CSV'
            File.write_file(txn_pool_initial_file_name,transaction_header)
            File.write_file(txn_pool_temporary_file_name,transaction_header)
            File.write_file(txn_pool_aborted_file_name,transaction_header)

    
        
    def remove_generated_files():
        File.remove_all_file_inside_directory('/storages/transactions/confirmed/')
        File.remove_all_file_inside_directory('/storages/transactions/temporary/')
        File.remove_all_file_inside_directory('/storages/transaction_pools/initial/')
        File.remove_all_file_inside_directory('/storages/transaction_pools/aborted/')
        File.remove_all_file_inside_directory('/storages/transaction_pools/temporary/')
        File.remove_file('/storages/GENERATED_ACCOUNTS.CSV')
    
    def remove_tmp_accounts_file():
        # remove the tmp accounts file
        tmp_account_save_file_path = '/storages/GENERATED_ACCOUNTS.CSV'
        File.remove_file(tmp_account_save_file_path)
    
    def create_storage_directory():
        File.create_directory('/storages/transaction_pools/temporary/rambo/')
    
    def remove_storage_directory():
        File.remove_directory('/storages/')