from config import SHARDS
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
                # each leader shard will have different transaction pool, safety & liveness should be considered
                process_transaction(transaction)


def process_transaction(transaction):
    print("Processing transaction ", transaction["TXN_ID"])
    for sub_transaction in split_transaction_to_sub_transactions(transaction, shards):
        # run "check" transactions in parallel
        print("Sub-transaction > ", sub_transaction)
        if (sub_transaction.type == "check"):
            success = check_balance(sub_transaction)
            if not success:
                print("Sub-transaction failed > ", sub_transaction)
                return

        elif (sub_transaction.type == "update"):
            success = update_balance(sub_transaction)
            # TODO once leader shard receives confirmation about update_balance, commit transaction
    print("Transaction completed successfully")


def check_balance(sub_transaction):
    print("Checking balance of account", sub_transaction.account_no,
          "on shard", sub_transaction.shard.id)
    # leader shard -> related shard -> check balance (amount, account no)
    # destination shard -> message recive, process, return
    # leader shard catch message
    # response true
    # checks balance of account on provided shard
    # this is dummy implementation of communication with shard
    return True  # TODO replace with transaction util method check_balance


def receive_command(message):
    print("A")
    # message decode =>command
    # command => check or update
    # validation
    # return response


def update_balance(sub_transaction):
    print("Updating balance of account", sub_transaction.account_no,
          "on shard", sub_transaction.shard.id)
    # Updates balance of account  on provided shard
    # this is dummy implementation of communication with shard
    return True  # TODO replace with transaction util method update_balance


def get_transactions_from_transaction_pool():
    # Currently there is single leader shard, so returning all transactions from pool, later transactions will be divided between leader shard in some basis [TBD]
    data = []
    with open('datas/GENERATED_TRANSACTIONS.CSV', encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)
        for row in csvReader:
            data.append(row)
    return data


def generate_shards():
    print("Setting up shards")
    shards.clear()  # clearing existing shards
    for shard in SHARDS:
        shards.append(Shard(shard[0], shard[1]))
    return shards

# process_transactions()
