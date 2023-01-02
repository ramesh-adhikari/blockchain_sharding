import datetime
import hashlib

from config import CONDITION_AND, CONDITION_HAS, SHARDS
from config import MESSAGE_DATA_SEPARATOR


class SubTransaction:
    def __init__(self,  type: str,txn_id: int, account_no: str,account_name: str, amount: int, shard: int, sub_txn_id: str, txn_timestamp:str) -> None:
        self.type = type  # check or update or commit_check or commit_update or abort_check or abort_update
        self.txn_id = txn_id
        self.account_no = account_no
        self.account_name = account_name
        self.amount = amount
        self.shard = shard
        self.sub_txn_id =sub_txn_id
        self.txn_timestamp =txn_timestamp


    def __str__(self):
        return self.to_message()

    def to_message(self):
        return self.type + MESSAGE_DATA_SEPARATOR \
        + self.txn_id + MESSAGE_DATA_SEPARATOR \
        + self.account_no + MESSAGE_DATA_SEPARATOR \
        + self.account_name + MESSAGE_DATA_SEPARATOR \
        + str(self.amount)+ MESSAGE_DATA_SEPARATOR \
        + str(self.shard)+MESSAGE_DATA_SEPARATOR\
        + str(self.sub_txn_id)+MESSAGE_DATA_SEPARATOR\
        + str(self.txn_timestamp)

    def change_type(self,type):
        return SubTransaction(type,self.txn_id,self.account_no,self.account_name,self.amount,self.shard,self.sub_txn_id,self.txn_timestamp)

    @staticmethod
    def from_message(message):
        message = message.split(MESSAGE_DATA_SEPARATOR)
        return SubTransaction(message[0],message[1],message[2],message[3],message[4],message[5],message[6],message[7])

def split_transaction_to_sub_transactions(transcation):
    transaction_id = transcation[0]
    txn_timestamp = transcation[5]
    sub_transactions = []

    for condition in transcation[4].split(CONDITION_AND):
        condition = condition.split(CONDITION_HAS)
        # sub-transaction to check condition
        sub_transactions.append(
            SubTransaction(
                "check",
                transaction_id,
                condition[0],
                condition[0].split('_')[1],
                condition[1],
                get_shard_for_account(condition[0]),
                generate_sub_transaction_id(condition[0]),
                txn_timestamp
            )
        )
    # sub-transaction to update balance of receiver
    sub_transactions.append(
        SubTransaction(
            "update",
            transaction_id,
            transcation[2],
            transcation[2].split('_')[1],
            transcation[3],
            get_shard_for_account(transcation[2]),
            generate_sub_transaction_id(transcation[2]),
            txn_timestamp
        )
    )
    # sub-transaction to update balance of sender
    sub_transactions.append(
        SubTransaction(
            "update",
            transaction_id,
            transcation[1],
            transcation[1].split('_')[1],
            -abs(int(transcation[3])),
            get_shard_for_account(transcation[1]),
           generate_sub_transaction_id(transcation[1]),
           txn_timestamp
        )
    )
    return sub_transactions

def generate_sub_transaction_id(sub_transaction):
    return 'SUB_TXN_' + hashlib.sha256((str(datetime.datetime.now()) + sub_transaction).encode()).hexdigest()

def get_shard_for_account(account_no):
    return int(account_no.rsplit('_', 1)[0]) % len(SHARDS)
