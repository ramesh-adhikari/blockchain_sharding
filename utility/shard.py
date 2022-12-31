from config import SHARDS

def get_shard_for_account(account_number):
    account_id = account_number.split("_")[0]
    return int(account_id)%len(SHARDS)


def get_number_of_leader_shards():
    total_number_of_leader=0
    for shard in SHARDS:
        if(shard[1]):
            total_number_of_leader = total_number_of_leader+1
    return total_number_of_leader