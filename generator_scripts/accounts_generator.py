import csv
import os
import sys
import string
import random

sys.path.append(os.path.abspath(os.curdir))
from  config import *

class AccountsGenerator:
    
    account_save_file_path = os.path.abspath(os.curdir)+'/datas/GENERATED_ACCOUNTS.CSV'

    account_header = ['ACCOUNT_NUMBER', 'ACCOUNT_NAME', 'AMOUNT']

    latters = string.ascii_uppercase

    with open(account_save_file_path, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(account_header)
        
        for na in range(NUMBER_OF_ACCOUNTS):
            name = ''.join(random.choices(string.ascii_uppercase, k=3))
            data = [str(na)+'_'+name, name, DEFAULT_AMOUNT]
            writer.writerow(data)