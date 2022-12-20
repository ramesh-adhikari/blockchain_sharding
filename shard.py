from config import NUMBER_OF_SHARDS
from models.shard import Shard
import csv

from models.sub_transaction import split_transaction_to_sub_transactions

shards = list()


def process_transactions():
    print("Processing transactions")
    generate_shards()
    for shard in shards:
        # looping through shards to find leader shards, later there will be multiple leader shards running in parallel
        if (shard.is_leader):
            print("Found leader shard", shard.id)
            for transaction in get_transactions_from_transaction_pool():
                process_transaction(transaction)


def process_transaction(transaction):
    print("Processing transaction ", transaction["TXN_ID"])
    for mini_transaction in split_transaction_to_sub_transactions(transaction, shards):
        print("Mini Transaction > ", mini_transaction)
        if (mini_transaction.type == "check"):
            success = check_balance(mini_transaction)
            if not success:
                print("Mini transaction failed > ", mini_transaction)
                continue

        elif (mini_transaction.type == "update"):
            success = update_balance(mini_transaction)
            # TODO once leader shard receives confirmation about update_balance, commit transaction


def check_balance(mini_transaction):
    print("Checking balance of account", mini_transaction.account_no,
          "on shard", mini_transaction.shard.id)
    # checks balance of account on provided shard
    # this is dummy implementation of communication with shard
    return False  # TODO replace with transaction util method check_balance


def update_balance(mini_transaction):
    print("Updating balance of account", mini_transaction.account_no,
          "on shard", mini_transaction.shard.id)
    # Updates balance of account  on provided shard
    # this is dummy implementation of communication with shard
    return True  # TODO replace with transaction util method update_balance


def get_transactions_from_transaction_pool():
    # Currently there is single leader shard, so returning all transactions from pool, later transactions will be divided between leader shard in some basis [TBD]
    data = []
    with open('datas/transactions.csv', encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)
        for row in csvReader:
            data.append(row)
    return data


def generate_shards():
    print("Setting up shards")
    shards.clear()  # clearing existing shards
    for i in range(0, NUMBER_OF_SHARDS):
        # for now first shard is leader shard, later multiple shards will be leader shard
        shards.append(Shard(i, i == 0))
    return shards


process_transactions()
