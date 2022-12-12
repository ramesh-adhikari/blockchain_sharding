import csv
import os
import sys
import string
import random

# import pandas as pd

sys.path.append(os.path.abspath(os.curdir))
from  config import *

class Transactions_Generator:
    transaction_save_file_path = os.path.abspath(os.curdir)+'/datas/transactions.csv'

    transaction_header = ['SENDER_ACCOUNT_ID', 'RECEIVER_ACCOUNT_ID', 'BALANCE', 'CONDITIONS']

    latters = string.ascii_uppercase

    account_save_file_path = os.path.abspath(os.curdir)+'/datas/accounts.csv'

        # print(df)

    with open(transaction_save_file_path, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(transaction_header)
        
        for lp in range(NUMBER_OF_TRANSACTIONS):
            with open(account_save_file_path) as account:
                reader = csv.reader(account)
                data = list(reader)
                
                from_row = data[random.randint(1,NUMBER_OF_ACCOUNTS)]
                to_row = data[random.randint(1,NUMBER_OF_ACCOUNTS)]
                conditions=''
                for con in range(NUMBER_OF_CONDITIONS):
                    single_account = data[random.randint(1,NUMBER_OF_ACCOUNTS)]
                    if con!=(NUMBER_OF_CONDITIONS-1):
                        conditions+=single_account[ACCOUNT_INDEX_ACCOUNT_ID]+CONDITION_HAS+single_account[ACCOUNT_INDEX_BALANCE]+CONDITION_AND
                    else:
                        conditions+=single_account[ACCOUNT_INDEX_ACCOUNT_ID]+CONDITION_HAS+single_account[ACCOUNT_INDEX_BALANCE]
                data = [from_row[ACCOUNT_INDEX_ACCOUNT_ID], to_row[ACCOUNT_INDEX_ACCOUNT_ID],from_row[ACCOUNT_INDEX_BALANCE],conditions]
                writer.writerow(data)