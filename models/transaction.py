import csv
import datetime
import os
import sys
import time
import pandas as pd
sys.path.append(os.path.abspath(os.curdir))
from generator_scripts.files_generator import FilesGenerator
from config import *
from utility.shard import get_shard_for_account
from utility.file import File


class Transaction:
    
    def append_sub_transaction_to_temporary_file(txn_id, sub_txn_id, account_number, account_name, amount,shard_id):
        print("Appending "+sub_txn_id)
        shard_file_path = FilesGenerator().get_txn_file_path(shard_id, 'temporary')
        timestamp = datetime.datetime.now()
        data = [txn_id, sub_txn_id, account_number, account_name, amount, timestamp]
        File.append_data(shard_file_path, data)
        print("Appended "+sub_txn_id)
        
        
    def move_sub_transaction_to_committed_transaction(shard_id, sub_txn_id):
        # move pending transaction to committed transaction
        print("Moving "+sub_txn_id)
        source_file = FilesGenerator().get_txn_file_path(shard_id, 'temporary')
        destination_file = FilesGenerator().get_txn_file_path(shard_id, 'committed')
        transaction = Transaction()
        transaction.move_transaction(source_file, destination_file, sub_txn_id, 'TRANSACTION')
        print("Moved "+sub_txn_id)
    
    def remove_transaction_from_temporary_transaction(shard_id, sub_txn_id):
        temporary_pool_txn_path = FilesGenerator().get_txn_file_path(shard_id, 'temporary')
        t_instance = Transaction()
        t_instance.delete_row_by_txn_id(temporary_pool_txn_path,sub_txn_id,'TRANSACTION')

    def remove_transaction_from_commited_transaction(shard_id, sub_txn_id):
        temporary_pool_txn_path = FilesGenerator().get_txn_file_path(shard_id, 'commited')
        t_instance = Transaction()
        t_instance.delete_row_by_txn_id(temporary_pool_txn_path,sub_txn_id,'TRANSACTION')

    def is_commited_transaction(shard_id, sub_txn_id):
        abs_file_path = os.path.abspath(os.curdir)+ FilesGenerator().get_txn_file_path(shard_id, 'committed')
        #TODO Check pd read implementation
        data = None
        while True:
            try:
                data = pd.read_csv(abs_file_path)
                break
            except:
                time.sleep(5/1000)
            
        if("SUB_TXN_ID" in data.index): #prevent key error
            selected_row = data.loc[(data["SUB_TXN_ID"] == sub_txn_id)]
            if(len(selected_row)>0):
                return True
        else:
            return False

    def is_temporary_transaction(shard_id, sub_txn_id):
        abs_file_path = os.path.abspath(os.curdir)+ FilesGenerator().get_txn_file_path(shard_id, 'temporary')
        #TODO Check pd read implementation
        data = None
        while True:
            try:
                data = pd.read_csv(abs_file_path)
                break
            except:
                time.sleep(5/1000)
        if("SUB_TXN_ID" in data.index): #prevent key error
            selected_row = data.loc[(data["SUB_TXN_ID"] == sub_txn_id)]
            if(len(selected_row)>0):
                return True
        else:
            return False
        
        
    
    def get_transactions_from_transaction_pool(shard_id):
        txn_pool_initial_file_name = os.path.abspath(os.curdir)+FilesGenerator().get_txn_pool_file_path(shard_id, 'initial')

        data = None
        while True:
            try:
                data = pd.read_csv(txn_pool_initial_file_name)
                break
            except:
                time.sleep(5/1000)

        # data = pd.read_csv(txn_pool_initial_file_name)
        # sort transaction as per timestamp
        data['TIMESTAMP'] = pd.to_datetime(data['TIMESTAMP'], format="%Y/%m/%d %H:%M")
        data = data.sort_values(by='TIMESTAMP', ascending=True)
        data.to_csv(txn_pool_initial_file_name,index=False)
        # print(data.to_csv(index=False))

        with open(txn_pool_initial_file_name, newline='') as f:
            reader = csv.reader(f)
            if(len(list(reader))>1):
                 with open(txn_pool_initial_file_name, newline='') as fn:
                    data_reader = csv.reader(fn)
                    row1= next(data_reader)
                    row2= next(data_reader)
                    Transaction().move_transaction_from_initial_to_temporary_pool(shard_id,row2[0])
                    return row2
            else:
                return None
    
    def move_transaction_from_initial_to_temporary_pool(self,shard_id, txn_id):
        transaction = Transaction()
        source_file = FilesGenerator().get_txn_pool_file_path(shard_id, 'initial')
        destination_file = FilesGenerator().get_txn_pool_file_path(shard_id, 'temporary')
        transaction.move_transaction(source_file, destination_file, txn_id)
        
    def move_transaction_from_temporary_to_abort_pool(shard_id, txn_id):
        transaction = Transaction()
        source_file = FilesGenerator().get_txn_pool_file_path(shard_id, 'temporary')
        destination_file = FilesGenerator().get_txn_pool_file_path(shard_id, 'aborted')
        transaction = Transaction()
        transaction.move_transaction(source_file, destination_file, txn_id)
    
    def move_transaction_from_temporary_to_initial_pool(shard_id, txn_id):
        transaction = Transaction()
        source_file = FilesGenerator().get_txn_pool_file_path(shard_id, 'temporary')
        destination_file = FilesGenerator().get_txn_pool_file_path(shard_id, 'initial')
        transaction.move_transaction(source_file, destination_file, txn_id)
    
    def move_transaction_from_temporary_to_committed_pool(shard_id, txn_id):
        transaction = Transaction()
        source_file = FilesGenerator().get_txn_pool_file_path(shard_id, 'temporary')
        destination_file = FilesGenerator().get_txn_pool_file_path(shard_id, 'committed')
        transaction.move_transaction(source_file, destination_file, txn_id)
    
    def remove_transaction_from_temporary_pool(shard_id, txn_id):
        transaction = Transaction()
        temporary_pool_txn_path = FilesGenerator().get_txn_pool_file_path(shard_id, 'temporary')
        t_instance = Transaction()
        t_instance.delete_row_by_txn_id(temporary_pool_txn_path,txn_id)
        
    
    def has_amount(account_number, amount):
        # check whether the account has sufficient balance or not
        shard_id = get_shard_for_account(account_number)
        shard_file_path = FilesGenerator().get_txn_file_path(shard_id, 'committed')
        shard_file_directory = os.path.abspath(os.curdir)+shard_file_path
        data_frame = pd.read_csv(shard_file_directory)
        # sum the amount associated with given account number
        account_amount = data_frame.loc[data_frame['ACCOUNT_NUMBER'] == account_number, 'AMOUNT'].sum()
        if(int(account_amount)>=int(amount)):
            return True
        return False    
    
    def move_transaction(self, source_file, destination_file, txn_id, type='TRANSACTION_POOL'):
        source_file = source_file
        destination_file = destination_file

        #TODO Check pd read implementation
        transaction = None
        while True:
            try:
                transaction = pd.read_csv(os.path.abspath(os.curdir)+source_file)
                break
            except:
                time.sleep(5/1000)

        if(type=='TRANSACTION'):
            selected_rows = transaction.loc[transaction['SUB_TXN_ID'] == txn_id]
        else:
            selected_rows = transaction.loc[transaction['TXN_ID'] == txn_id]
        if(len(selected_rows)<1):
            print('row not found in source file '+ source_file+ 'for '+type+ 'with ID '+ txn_id)
        else:
            print('row  found in source file '+ source_file+ 'for '+type+ 'with ID '+ txn_id + 'and moves to destination file' + destination_file)

        t_instance = Transaction()
        move_data = t_instance.get_move_data(selected_rows, type)
        File.append_data(destination_file,move_data)
        
        # delete row from temporary
        if(type=='TRANSACTION'):
            t_instance.delete_row_by_txn_id(source_file,selected_rows['SUB_TXN_ID'][selected_rows.index[0]], type)
        else:
            t_instance.delete_row_by_txn_id(source_file,selected_rows['TXN_ID'][selected_rows.index[0]], type)

        
    def get_move_data(self, row, type):
        if(type=='TRANSACTION_POOL'):
           return [row['TXN_ID'][row.index[0]],   
                     row['SENDER_ACCOUNT_NUMBER'][row.index[0]],
                     row['RECEIVER_ACCOUNT_NUMBER'][row.index[0]],
                     row['AMOUNT'][row.index[0]],
                     row['CONDITIONS'][row.index[0]],
                     row['TIMESTAMP'][row.index[0]]
                    ] 
        elif(type=='TRANSACTION'):
            return [row['TXN_ID'][row.index[0]],
                     row['SUB_TXN_ID'][row.index[0]],
                     row['ACCOUNT_NUMBER'][row.index[0]],
                     row['ACCOUNT_NAME'][row.index[0]],
                     row['AMOUNT'][row.index[0]],
                     row['TIMESTAMP'][row.index[0]]
                     ]
    
    def delete_row_by_txn_id(self, file_path, txn_id, type):
        abs_file_path = os.path.abspath(os.curdir)+file_path
        #TODO Check pd read implementation
        transaction = None
        while True:
            try:
                transaction = pd.read_csv(abs_file_path)
                break
            except:
                time.sleep(5/1000)

        if(type=='TRANSACTION'):
            transaction.drop(transaction.index[(transaction["SUB_TXN_ID"] == txn_id)],axis=0,inplace=True)
        else:
            transaction.drop(transaction.index[(transaction["TXN_ID"] == txn_id)],axis=0,inplace=True)

        transaction.to_csv(abs_file_path,index=False)
    

    # Lock
    def append_account_to_lock_file(shard_id, account_number,txn_shard_id, timestamp):
        if(TRANSACTION_TYPE=='LOCK'):
            print("Locking "+account_number)
            shard_file_path = FilesGenerator().get_txn_file_path(shard_id, 'lock')
            data = [account_number,txn_shard_id,timestamp]
            File.append_data(shard_file_path, data)
            print("Locked "+account_number)
        else:
            return
    
    def is_account_locked(shard_id,txn_shard_id, account_number):
        if(TRANSACTION_TYPE=='LOCK'):
            shard_file_path = FilesGenerator().get_txn_file_path(shard_id, 'lock')
            shard_file_directory = os.path.abspath(os.curdir)+shard_file_path
            #TODO Check pd read implementation
            data_frame = None
            while True:
                try:
                    data_frame = pd.read_csv(shard_file_directory)
                    break
                except:
                    time.sleep(50/1000)

            account = data_frame.loc[data_frame['ACCOUNT_NUMBER'] == account_number]
            if(len(account)>0):
                if(account['TRANSACTION_SHARD_ID'] [account.index[0]]==txn_shard_id):
                    return False
                else:
                    return True
            return False  
        else:
            return False

    def remove_account_lock(shard_id, account_number):
        if(TRANSACTION_TYPE=='LOCK'):
            print("Unlocking "+account_number)
            abs_file_path = os.path.abspath(os.curdir)+ FilesGenerator().get_txn_file_path(shard_id, 'lock')
            #TODO Check pd read implementation
            account = None
            while True:
                try:
                    account = pd.read_csv(abs_file_path)
                    break
                except:
                    time.sleep(5/1000)
            # if ("ACCOUNT_NUMBER" in account.index):
            account.drop(account.index[(account["ACCOUNT_NUMBER"] == account_number)],axis=0,inplace=True)
            account.to_csv(abs_file_path,index=False)
            print("Unlocked "+account_number)
            # else:
            #     print("Lock for account "+account_number+" not found in shard "+str(shard_id))
        else:
            return
        
    # version control
    def append_data_to_snapshot(shard_id, txn_id, sub_txn_id, account_number,txn_shard_id, txn_generated_timestamp):
        if(TRANSACTION_TYPE=='OUR_PROTOCOL'):
            shard_file_path = FilesGenerator().get_txn_file_path(shard_id, 'snapshot')
            data = [shard_id, txn_id, sub_txn_id, account_number, txn_shard_id, txn_generated_timestamp,'ACQUIRE']
            File.append_data(shard_file_path, data)
            print('Transaction with txn ID: '+ txn_id + 'and sub txn ID: '+sub_txn_id+ 'take snapshot on account '+account_number + ' ACQUIRE' )
        else:
            return
    
    def remove_snapshot(shard_id, txn_id, sub_txn_id, account_no,txn_shard_id,txn_generated_timestamp):
        if(TRANSACTION_TYPE=='OUR_PROTOCOL'):
            shard_file_path = FilesGenerator().get_txn_file_path(shard_id, 'snapshot')
            data = [shard_id, txn_id, sub_txn_id, account_no, txn_shard_id, txn_generated_timestamp,'RELEASED']
            snapshot = None
            while True:
                try:
                    snapshot = pd.read_csv(os.path.abspath(os.curdir)+shard_file_path)
                    break
                except:
                    time.sleep(5/1000)
            selected_row = snapshot.loc[(snapshot["ACCOUNT_NUMBER"] == account_no) & (snapshot["SUB_TXN_ID"] == sub_txn_id)].tail(1)
           
            if(len(selected_row)):
                if(selected_row['TYPE'] [selected_row.index[0]]=="ACQUIRE"):
                    File.append_data(shard_file_path, data)
                    print('Transaction with txn ID: '+ txn_id + 'and sub txn ID: '+sub_txn_id+ 'remove snapshot on account '+account_no + ' RELEASED' )
                    return
                else:
                    print('Transaction with txn ID: '+ txn_id + 'and sub txn ID: '+sub_txn_id+ 'remove snapshot on account '+account_no + ' ALREADY_RELEASED' )
                    return
            else:
                print('Transaction with txn ID: '+ txn_id + 'and sub txn ID: '+sub_txn_id+ ' tried to remove snapshot on account '+account_no + ' NOT_ACQUIRED_YET' )
                return
            
            return

            abs_file_path = os.path.abspath(os.curdir)+ FilesGenerator().get_txn_file_path(shard_id, 'snapshot')
            #TODO Check pd read implementation
            snapshot = None
            while True:
                try:
                    snapshot = pd.read_csv(abs_file_path)
                    break
                except:
                    time.sleep(5/1000)

            #TODO Do we really need sub_txn_id here?
            snapshot.drop(snapshot.index[(snapshot["SUB_TXN_ID"] == sub_txn_id) & (snapshot["ACCOUNT_NUMBER"] == account_no)],axis=0,inplace=True)
            # snapshot.drop(snapshot.index[ (snapshot["ACCOUNT_NUMBER"] == account_no)],axis=0,inplace=True)
            snapshot.to_csv(abs_file_path,index=False)
        else:
            return
    
    def get_row_from_snapshot(shard_id, account_no):
        if(TRANSACTION_TYPE=='OUR_PROTOCOL'):
            abs_file_path = os.path.abspath(os.curdir)+ FilesGenerator().get_txn_file_path(shard_id, 'snapshot')
            #TODO Check pd read implementation
            snapshot = None
            while True:
                try:
                    snapshot = pd.read_csv(abs_file_path)
                    break
                except:
                    time.sleep(5/1000)
               
            selected_row = snapshot.loc[(snapshot["ACCOUNT_NUMBER"] == account_no)].tail(1)
           
            if(len(selected_row)>0):
                if(selected_row['TYPE'] [selected_row.index[0]]=="RELEASED"):
                    return None
                else:
                    return [
                                selected_row['SHARD_ID'] [selected_row.index[0]],
                                selected_row['TXN_ID'] [selected_row.index[0]],
                                selected_row['SUB_TXN_ID'] [selected_row.index[0]],
                                selected_row['ACCOUNT_NUMBER'] [selected_row.index[0]],
                                selected_row['TRANSACTION_SHARD_ID'] [selected_row.index[0]],
                                selected_row['TRANSACTION_GENERATED_TIMESTAMP'] [selected_row.index[0]],
                            ]
            else:
                return None
        else:
            return None
    
    def get_timestamp_from_last_row_of_committed_txn(shard_id, account_no):
        if(TRANSACTION_TYPE=='OUR_PROTOCOL'):
            abs_file_path = os.path.abspath(os.curdir)+ FilesGenerator().get_txn_file_path(shard_id, 'committed')
            snapshot = pd.read_csv(abs_file_path)
            selected_row = snapshot.loc[(snapshot["ACCOUNT_NUMBER"] == account_no)].tail(1)
            return selected_row['TIMESTAMP'][selected_row.index[0]]
        else:
            return