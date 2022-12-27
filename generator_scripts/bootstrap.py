
from generator_scripts.accounts_and_transaction_generator import AccountsAndTransactionGenerator
from generator_scripts.files_generator import FilesGenerator


class Bootstrap:
    
    def run():
        # this method do the following
        # remove existing files to make fresh transaction
        FilesGenerator.remove_storage_directory()
        FilesGenerator.create_storage_directory()
        # generate the transaction files, temporary account files, transaction pool file and set the header of each csv file
        # Generate the account file
        FilesGenerator.create_shard_transaction_file()
        FilesGenerator.create_tmp_account_file()
        FilesGenerator.create_transaction_pool_file()
        AccountsAndTransactionGenerator.create_tmp_accounts_file()
         # assign each assocunts to some shards
        # generate the transaction and append these transaction to transaction pool
        #  remove temporary account generted file
        AccountsAndTransactionGenerator.assign_accounts_to_shard()
        AccountsAndTransactionGenerator.create_transaction_and_append_to_transaction_pool()
        FilesGenerator.remove_tmp_accounts_file()