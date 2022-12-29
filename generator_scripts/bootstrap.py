
from generator_scripts.accounts_and_transaction_generator import AccountsAndTransactionGenerator
from generator_scripts.files_generator import FilesGenerator


class Bootstrap:
    
    def run():
        # this method do the following
        # remove existing files to make fresh transaction
        print('############ Start to generate initial files and storages...########')
        FilesGenerator.remove_storage_directory()
        
        FilesGenerator.create_storage_directory()
        print('Created storage directory')
        # generate the transaction files, temporary account files, transaction pool file and set the header of each csv file
        # Generate the account file
       
        FilesGenerator.create_shard_transaction_file()
        print("created shard transaction files")

        FilesGenerator.create_tmp_account_file()
        print("created temporary account files")

        FilesGenerator.create_transaction_pool_file()
        print("created transaction pool files")

        AccountsAndTransactionGenerator.create_tmp_accounts_file()
        print("Generated temporary accounts")
        # assign each assocunts to some shards
        # generate the transaction and append these transaction to transaction pool
        #  remove temporary account generted file
        AccountsAndTransactionGenerator.assign_accounts_to_shard()
        print("Assigned each accounts to respective shards")

        AccountsAndTransactionGenerator.create_transaction_and_append_to_transaction_pool()
        print("Generate transaction and append to transaction pool")

        FilesGenerator.remove_tmp_accounts_file()
        print('############ Completed generation of initial files and storages...########')