import csv
import os
import sys
import string
import random

sys.path.append(os.path.abspath(os.curdir))
from  config import *

class TransactionsGenerator:
    transaction_save_file_path = os.path.abspath(os.curdir)+'/datas/GENERATED_TRANSACTIONS.CSV'

    transaction_header = ['TXN_ID', 'SENDER_ACCOUNT_NUMBER', 'RECEIVER_ACCOUNT_NUMBER', 'AMOUNT', 'CONDITIONS']

    latters = string.ascii_uppercase

    account_save_file_path = os.path.abspath(os.curdir)+'/datas/GENERATED_ACCOUNTS.CSV'

    with open(transaction_save_file_path, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(transaction_header)
        
        for nt in range(NUMBER_OF_TRANSACTIONS):
            with open(account_save_file_path) as account:
                reader = csv.reader(account)
                data = list(reader)
                from_row = data[random.randint(1,NUMBER_OF_ACCOUNTS)]
                to_row = data[random.randint(1,NUMBER_OF_ACCOUNTS)]
                conditions=''
                for con in range(NUMBER_OF_CONDITIONS):
                    single_account = data[random.randint(1,NUMBER_OF_ACCOUNTS)]
                    if con!=(NUMBER_OF_CONDITIONS-1):
                        conditions+=single_account[ACCOUNT_INDEX_ACCOUNT_NUMBER]+CONDITION_HAS+single_account[ACCOUNT_INDEX_AMOUNT]+CONDITION_AND
                    else:
                        conditions+=single_account[ACCOUNT_INDEX_ACCOUNT_NUMBER]+CONDITION_HAS+single_account[ACCOUNT_INDEX_AMOUNT]
                data = ['TXN_'+str(nt),from_row[ACCOUNT_INDEX_ACCOUNT_NUMBER], to_row[ACCOUNT_INDEX_ACCOUNT_NUMBER],from_row[ACCOUNT_INDEX_AMOUNT],conditions]
                writer.writerow(data)