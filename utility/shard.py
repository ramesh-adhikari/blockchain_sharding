from config import NUMBER_OF_SHARDS

def get_shard_for_account(account_number):
    account_id = account_number.split("_")[0]
    return int(account_id)%NUMBER_OF_SHARDS