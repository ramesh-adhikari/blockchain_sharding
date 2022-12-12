import csv
import os
import sys
import string
import random

sys.path.append(os.path.abspath(os.curdir))
from  config import *

class Accounts_Generator:
    
    account_save_file_path = os.path.abspath(os.curdir)+'/datas/accounts.csv'

    account_header = ['ACCOUNT_ID', 'ACCOUNT_NAME', 'AMOUNT']

    latters = string.ascii_uppercase

    with open(account_save_file_path, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(account_header)
        
        for lp in range(NUMBER_OF_ACCOUNTS):
            name = ''.join(random.choices(string.ascii_uppercase, k=3))
            data = [str(lp)+'_'+name, name, DEFAULT_AMOUNT]
            writer.writerow(data)