from config import CONDITION_AND, CONDITION_HAS
from models.shard import Shard


class SubTransaction:
    def __init__(self, type: str, account_no: str, amount: int, shard: Shard) -> None:
        self.type = type
        self.account_no = account_no
        self.amount = amount
        self.shard = shard

    def __str__(self):
        return "Type : "+self.type+", Account No : "+self.account_no+", Amount : "+str(self.amount)+", Shard : "+str(self.shard.id)


def split_transaction_to_sub_transactions(transcation, shards):
    print("Splitting transaction",
          transcation["TXN_ID"], "to sub transactions.")
    sub_transactions = []
    # sub-transaction to check if sender has balance
    sub_transactions.append(
        SubTransaction(
            "check",
            transcation["SENDER_ACCOUNT_ID"],
            transcation["AMOUNT"],
            get_shard_for_account(transcation["SENDER_ACCOUNT_ID"], shards)
        )
    )

    for condition in transcation["CONDITIONS"].split(CONDITION_AND):
        condition = condition.split(CONDITION_HAS)
        # sub-transaction to check condition
        sub_transactions.append(
            SubTransaction(
                "check",
                condition[0],
                condition[1],
                get_shard_for_account(condition[0], shards)
            )
        )
    # sub-transaction to update balance of receiver
    sub_transactions.append(
        SubTransaction(
            "update",
            transcation["RECEIVER_ACCOUNT_ID"],
            transcation["AMOUNT"],
            get_shard_for_account(transcation["RECEIVER_ACCOUNT_ID"], shards)
        )
    )
    # sub-transaction to update balance of sender
    sub_transactions.append(
        SubTransaction(
            "update",
            transcation["SENDER_ACCOUNT_ID"],
            transcation["AMOUNT"],
            get_shard_for_account(transcation["SENDER_ACCOUNT_ID"], shards)
        )
    )
    print("Transaction splitted to", str(
        len(sub_transactions)), "sub-transactions")
    return sub_transactions


def get_shard_for_account(account_no, shards):
    return shards[hash(account_no) % len(shards)]
