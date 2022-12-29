import datetime
import hashlib

from config import CONDITION_AND, CONDITION_HAS, SHARDS
from config import MESSAGE_DATA_SEPARATOR


class SubTransaction:
    def __init__(self, txn_id: int, type: str, account_no: str, amount: int, shard: int, sub_txn_id: str) -> None:
        self.txn_id = txn_id
        self.type = type  # check or update
        self.account_no = account_no
        self.amount = amount
        self.shard = shard
        self.sub_txn_id = sub_txn_id

    def __str__(self):
        return "TXN ID : " + str(
            self.txn_id) + ", Type: " + self.type + ", Account No: " + self.account_no + ", Amount: " + str(
            self.amount) + ", Shard: " + str(self.shard) + "SUB_TXN_ID :" + str(self.sub_txn_id)

    def to_message(self):
        return self.type + MESSAGE_DATA_SEPARATOR + self.account_no + MESSAGE_DATA_SEPARATOR + str(self.amount)+ MESSAGE_DATA_SEPARATOR + self.txn_id + MESSAGE_DATA_SEPARATOR + self.sub_txn_id+MESSAGE_DATA_SEPARATOR + str(self.shard)

    #TODO add method to convert message to model

def split_transaction_to_sub_transactions(transcation):
    transaction_id = transcation[0]
    sub_transactions = []

    for condition in transcation[4].split(CONDITION_AND):
        condition = condition.split(CONDITION_HAS)
        # sub-transaction to check condition
        sub_transactions.append(
            SubTransaction(
                transaction_id,
                "check",
                condition[0],
                condition[1],
                get_shard_for_account(condition[0]),
                'SUB_TXN_' + hashlib.sha256((str(datetime.datetime.now()) + condition[0]).encode()).hexdigest()
            )
        )
    # sub-transaction to update balance of receiver
    sub_transactions.append(
        SubTransaction(
            transaction_id,
            "update",
            transcation[2],
            transcation[3],
            get_shard_for_account(transcation[2]),
            'SUB_TXN_' + hashlib.sha256((str(datetime.datetime.now()) + transcation[2]).encode()).hexdigest()
        )
    )
    # sub-transaction to update balance of sender
    sub_transactions.append(
        SubTransaction(
            transaction_id,
            "update",
            transcation[1],
            -abs(int(transcation[3])),
            get_shard_for_account(transcation[1]),
            'SUB_TXN_' + hashlib.sha256((str(datetime.datetime.now()) + transcation[1]).encode()).hexdigest()
        )
    )
    return sub_transactions


def get_shard_for_account(account_no):
    return int(account_no.rsplit('_', 1)[0]) % len(SHARDS)
