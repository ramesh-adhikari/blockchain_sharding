
from generator_scripts.accounts_and_transaction_generator import AccountsAndTransactionGenerator
from generator_scripts.files_generator import FilesGenerator


class Bootstrap:
    
    def run():
        # this method do the following
        # remove existing files to make fresh transaction
        print("\n")
        print("---------------- START TO GENERATE INITIAL FILES AND STORAGES STARTING ... -------------------")
        FilesGenerator.remove_storage_directory()
        
        FilesGenerator.create_storage_directory()
        print('Created storage directory')
        # generate the transaction files, temporary account files, transaction pool file and set the header of each csv file
        # Generate the account file
       
        FilesGenerator.create_shard_transaction_file()
        print("Created shard transaction files")

        FilesGenerator.create_tmp_account_file()
        print("Created temporary account files")

        FilesGenerator.create_transaction_pool_file()
        print("Created transaction pool files")

        AccountsAndTransactionGenerator.create_tmp_accounts_file()
        print("Generated temporary accounts")
        # assign each assocunts to some shards
        # generate the transaction and append these transaction to transaction pool
        #  remove temporary account generted file
        AccountsAndTransactionGenerator.assign_accounts_to_shard()
        print("Assigned each accounts to respective shards")

        AccountsAndTransactionGenerator.create_transaction_and_append_to_transaction_pool()
        print("Generated transactions and appended to transaction pool")

        FilesGenerator.remove_tmp_accounts_file()
        print("Removed temporary account files")
        print("---------------- COMPLETED GENERATION OF INITIAL FILES AND STORAGES !!! --------------------")
        print("\n")