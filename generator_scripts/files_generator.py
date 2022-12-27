import sys
import os

sys.path.append(os.path.abspath(os.curdir))
from config import *
from utility.file import File

class FilesGenerator:
    
    # This function create the n (n= number of shards) temporary and committed files to store the transaction
    def create_shard_transaction_file():
        transaction_header = ['TXN_ID', 'SUB_TXN_ID', 'ACCOUNT_NUMBER', 'ACCOUNT_NAME', 'AMOUNT', 'TIMESTAMP']
        for n_shard in range(len(SHARDS)):
            tmp_file_name = FilesGenerator().get_txn_file_path(n_shard, 'temporary')
            confirm_file_name = FilesGenerator().get_txn_file_path(n_shard, 'committed')
            File.write_file(tmp_file_name,transaction_header)
            File.write_file(confirm_file_name,transaction_header)
    
    def create_tmp_account_file():
        tmp_account_save_file_path = '/storages/GENERATED_ACCOUNTS.CSV'
        account_header = ['ACCOUNT_NUMBER', 'ACCOUNT_NAME', 'AMOUNT']
        File.write_file(tmp_account_save_file_path, account_header)
    
    def create_transaction_pool_file():
  
        transaction_header = ['TXN_ID', 'SENDER_ACCOUNT_NUMBER', 'RECEIVER_ACCOUNT_NUMBER', 'AMOUNT', 'CONDITIONS', 'TIMESTAMP']
        for n_leader in range(NUMBER_OF_LEADER_SHARD):
            txn_pool_initial_file_name = FilesGenerator().get_txn_pool_file_path(n_leader, 'initial')
            txn_pool_temporary_file_name = FilesGenerator().get_txn_pool_file_path(n_leader, 'temporary')
            txn_pool_aborted_file_name =  FilesGenerator().get_txn_pool_file_path(n_leader, 'aborted')
            txn_pool_committed_file_name =  FilesGenerator().get_txn_pool_file_path(n_leader, 'committed')
            File.write_file(txn_pool_initial_file_name,transaction_header)
            File.write_file(txn_pool_temporary_file_name,transaction_header)
            File.write_file(txn_pool_aborted_file_name,transaction_header)
            File.write_file(txn_pool_committed_file_name,transaction_header)
    
    def remove_tmp_accounts_file():
        # remove the tmp accounts file
        tmp_account_save_file_path = '/storages/GENERATED_ACCOUNTS.CSV'
        File.remove_file(tmp_account_save_file_path)
    
    def create_storage_directory():
        for n_shard in range(len(SHARDS)):
            File.create_directory('/storages/shards/'+str(n_shard)+'/transactions/committed/')
            File.create_directory('/storages/shards/'+str(n_shard)+'/transactions/temporary/')
            File.create_directory('/storages/shards/'+str(n_shard)+'/transactions/pools/initial/')
            File.create_directory('/storages/shards/'+str(n_shard)+'/transactions/pools/aborted/')
            File.create_directory('/storages/shards/'+str(n_shard)+'/transactions/pools/temporary/')
            File.create_directory('/storages/shards/'+str(n_shard)+'/transactions/pools/committed/')
    
    def remove_storage_directory():
        File.remove_directory('/storages/')
    

    def get_txn_pool_file_path(self, shard_id, rel_directory):
        return '/storages/shards/'+str(shard_id)+'/transactions/pools/'+rel_directory+'/'+TRANSACTION_FILE_NAME
    
    def get_txn_file_path(self, shard_id, rel_directory):
        return '/storages/shards/'+str(shard_id)+'/transactions/'+rel_directory+'/'+TRANSACTION_FILE_NAME