from config import CONDITION_AND, CONDITION_HAS, NUMBER_OF_SHARDS
from models.shard import Shard


class SubTransaction:
    def __init__(self, txn_id: int, type: str, account_no: str, amount: int, shard: int) -> None:
        self.txn_id = txn_id
        self.type = type  # check or update
        self.account_no = account_no
        self.amount = amount
        self.shard = shard

    def __str__(self):
        return "TXN ID : "+str(self.txn_id)+", Type: "+self.type+", Account No: "+self.account_no+", Amount: "+str(self.amount)+", Shard: "+str(id)

    def to_message(self):
        return (self.type+"__"+self.account_no+"__"+str(self.amount))


def split_transaction_to_sub_transactions(transcation):

    transcation_id = transcation[0]
    sub_transactions = []

    for condition in transcation[4].split(CONDITION_AND):
        condition = condition.split(CONDITION_HAS)
        # sub-transaction to check condition
        sub_transactions.append(
            SubTransaction(
                transcation_id,
                "check",
                condition[0],
                condition[1],
                get_shard_for_account(condition[0])
            )
        )
    # sub-transaction to update balance of receiver
    sub_transactions.append(
        SubTransaction(
            transcation_id,
            "update",
            transcation[2],
            transcation[3],
            get_shard_for_account(transcation[2])
        )
    )
    # sub-transaction to update balance of sender
    print(" ====== sdjkfjknsdjkfslnfsdknjf "+str(transcation[3]))
    sub_transactions.append(
        SubTransaction(
            transcation_id,
            "update",
            transcation[1],
            -abs(int(transcation[3])),
            get_shard_for_account(transcation[1])
        )
    )
    return sub_transactions


def get_shard_for_account(account_no):
    return int(account_no.rsplit('_', 1)[0]) % NUMBER_OF_SHARDS
