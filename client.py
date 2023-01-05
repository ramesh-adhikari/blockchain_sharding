import socket
import time
from config import ACCOUNT_LOCK_RETRY_TIME_MS, HOST, MESSAGE_DATA_SEPARATOR, MESSAGE_SEPARATOR, TRANSACTION_FILE_NAME, TRANSACTION_TYPE
from models.sub_transaction import SubTransaction
from models.transaction import Transaction

client_socket = None
leader_shard_id = 0
shard_id = 0
terminate_transaction_processing = False


def init_client(s_id, port, leader_s_id):
    global client_socket, shard_id, leader_shard_id
    shard_id = s_id
    leader_shard_id = leader_s_id

    client_socket = socket.socket()
    try:
        client_socket.connect((HOST, port))
    except socket.error as e:
        time.sleep(100/1000)
        init_client(s_id, port, leader_s_id)

    while True:
        if terminate_transaction_processing:
            break
        for response in decode_response_from_server(client_socket):
            handle_response_from_server(response)

    close_socket()


def decode_response_from_server(client_socket):
    response_list = client_socket.recv(2048).decode().split(MESSAGE_SEPARATOR)
    response_list.pop()
    return response_list


def handle_response_from_server(response):
    global terminate_transaction_processing
    print("Leader shard "+str(leader_shard_id) +
          " > Shard "+str(shard_id)+" : "+response)
    if (response == "send_shard_id"):
        c_host, c_port = client_socket.getsockname()
        print("Shard "+str(shard_id)+" will communicate with leader shard " +
              str(leader_shard_id) + " in port "+str(c_port))
        send_message("shard_id_"+str(shard_id))
    elif response.startswith("check"):
        check_balance(response)
    elif response.startswith("update"):
        update_balance(response)
    elif response.startswith("commit"):
        commit_transaction(response)
    elif response.startswith("abort"):
        abort_transaction(response)
    elif response.startswith("rollback"):
        abort_rollback_transaction(response)
    elif response.startswith("release"):
        release_snapshot(response)
    elif response.startswith("terminate_transaction_processing"):
        terminate_transaction_processing = True


def check_balance(response):
    sub_transaction: SubTransaction = SubTransaction.from_message(response)
    success = Transaction.has_amount(
        sub_transaction.account_no, sub_transaction.amount)
    if success:
        if check_snapshot(sub_transaction):
            check_lock(sub_transaction.account_no, sub_transaction.txn_id,
                       sub_transaction.sub_txn_id, sub_transaction.txn_timestamp)
            send_message("vote_commit"+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id +
                         MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id)
    else:
        send_message("vote_abort"+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id +
                     MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id)


def update_balance(response):
    sub_transaction: SubTransaction = SubTransaction.from_message(response)
    if check_snapshot(sub_transaction):
        check_lock(sub_transaction.account_no, sub_transaction.txn_id,
                   sub_transaction.sub_txn_id, sub_transaction.txn_timestamp)
        success = Transaction.has_amount(
            sub_transaction.account_no, sub_transaction.amount)
        if success:
            Transaction.append_sub_transaction_to_temporary_file(
                sub_transaction.txn_id, sub_transaction.sub_txn_id, sub_transaction.account_no, sub_transaction.account_name, sub_transaction.amount, shard_id)
            send_message("vote_commit"+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id +
                         MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id)
        else:
            send_message("vote_abort"+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id +
                         MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id)


def commit_transaction(response):
    sub_transaction: SubTransaction = SubTransaction.from_message(response)
    if check_snapshot(sub_transaction):
        if (sub_transaction.type == "commit_update"):
            Transaction.move_sub_transaction_to_committed_transaction(
                shard_id, sub_transaction.sub_txn_id)
        send_message("committed"+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id +
                     MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id)
        release_lock(sub_transaction.account_no)


def abort_transaction(response):
    sub_transaction: SubTransaction = SubTransaction.from_message(response)
    if (sub_transaction.type == "abort_update"):
        Transaction.remove_transaction_from_temporary_transaction(
            shard_id, sub_transaction.sub_txn_id)
    send_message("aborted"+MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id)
    release_snapshot(response)
    release_lock(sub_transaction.account_no)


def abort_rollback_transaction(response):
    sub_transaction: SubTransaction = SubTransaction.from_message(response)
    if (sub_transaction.type == "rollback_update"):
        Transaction.remove_transaction_from_temporary_transaction(
            shard_id, sub_transaction.sub_txn_id)
    send_message("rollbacked"+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id +
                 MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id)
    release_snapshot(response)
    release_lock(sub_transaction.account_no)


def send_message(message):
    client_socket.send((message+MESSAGE_SEPARATOR).encode())


def close_socket():
    global client_socket
    client_socket.close()


def check_snapshot(sub_transaction: SubTransaction):
    if (TRANSACTION_TYPE != 'OUR_PROTOCOL'):
        return True
    snapshot = Transaction.get_row_from_snapshot(
        shard_id, sub_transaction.account_no)
    if (snapshot == None):
        Transaction.append_data_to_snapshot(
            shard_id, sub_transaction.txn_id, sub_transaction.sub_txn_id, sub_transaction.account_no, sub_transaction.txn_timestamp)
        return True
    elif (snapshot[2] == sub_transaction.sub_txn_id):
        return True
    else:
        snapshot_timestamp = snapshot[4]
        if (snapshot_timestamp > sub_transaction.txn_timestamp):
            # TODO instead of passed sub_txn_id and txn_id, send txn_id and sub_txn_id from snapshot
            print(
                " ==== ==== ===== ==== ==== === ====  Send message to another leader shard "+str(snapshot))
            # send_message("vote_rollback"+MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id)
            return True
        else:
            send_message("vote_rollback"+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id +
                         MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id)
            return False


def release_snapshot(response):
    if (TRANSACTION_TYPE != 'OUR_PROTOCOL'):
        return True
    sub_transaction: SubTransaction = SubTransaction.from_message(response)
    Transaction.remove_snapshot(
        shard_id, sub_transaction.sub_txn_id, sub_transaction.account_no)


def check_lock(account_number, txn_id, sub_txn_id, txn_timestamp):
    wait_for_lock(account_number)
    lock_account(account_number, txn_id, sub_txn_id, txn_timestamp)


def wait_for_lock(account_number):
    if (TRANSACTION_TYPE != 'LOCK'):
        return
    while True:
        locked = Transaction.is_account_locked(shard_id, account_number)
        print("Account "+account_number+" is currenlty locked in shard "+str(shard_id) +
              ". Will retry in "+str(ACCOUNT_LOCK_RETRY_TIME_MS)+" millisecond.")
        if (locked):
            time.sleep(ACCOUNT_LOCK_RETRY_TIME_MS/1000)
            continue
        else:
            break


def lock_account(account_number, txn_id, sub_txn_id, txn_timestamp):
    if (TRANSACTION_TYPE != 'LOCK'):
        return
    time.sleep(100/1000)
    print("Account lock applied to "+account_number+" in shard " +
          str(shard_id)+" txn id : "+txn_id+" sub txn id : "+sub_txn_id)
    Transaction.append_account_to_lock_file(
        shard_id, account_number, txn_timestamp)


def release_lock(account_number):
    if (TRANSACTION_TYPE != 'LOCK'):
        return
    print("Account lock released to "+account_number+" in shard "+str(shard_id))
    Transaction.remove_account_lock(shard_id, account_number)
